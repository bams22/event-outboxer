# 2026-09-04 (tenth session) — the bounded lock wait of ADR-0035, and its default

**Qualifies for README numbers: no.** Same laptop as the
[first session](2026-09-04-laptop-first-run.md); PostgreSQL and Redis
in standalone containers on the same host. Read the cells against each
other.

## Question

ADR-0035 proposes that a handler thread which finds an entity lock busy
waits for it — bounded by a per-type `lock-wait` — instead of releasing
the event to the back of the backlog at once. The ADR shipped with the
wait implemented behind `lock-wait: 0` and a validation plan: which
value, if any, should be the default? The plan names five cells (the
`hot-key` preset at several waits, one JVM versus three, `throughput`
without keys, `crash`, the virtual executor) and demands a sixth before
any default is fixed: a mixed workload with one slow hot key next to
many cool ones.

## Setup

| | |
|---|---|
| Host, JVM, PostgreSQL, Redis | as in the [first](2026-09-04-laptop-first-run.md) and [locks](2026-09-04-laptop-locks.md) sessions: Intel Core i7-13700H, Temurin 25.0.2, PostgreSQL 15.17 (`shared_buffers=1GB`, `fsync=on`) and Redis 7.4.7, each in a standalone container on the same host |
| Library | the two ADR-0035 commits (`b6290e5` feature, `258fcf2` advisory native wait) with `lock-wait: 0` as the shipped default; every cell sets the wait explicitly through `--bench.worker-prop.` |
| Harness | this session's commit, which adds `--bench.slow-key-share` / `--bench.slow-key-work-time` for the mixed cell |
| Fleet | in-process except `crash` (forked, as the preset requires) |
| Method | one run per cell, no warm-up, steady state; `VACUUM FULL` at every run start |

```bash
JAR=event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar
DB="--bench.jdbc-url=jdbc:postgresql://127.0.0.1:55432/bench --bench.jdbc-user=bench --bench.jdbc-password=bench"
LW=--bench.worker-prop.event-outboxer.event-types.defaults.lock-wait
for w in 0 20ms 100ms 500ms; do java -jar $JAR $DB --bench.scenario=hot-key $LW=$w; done
for w in 0 100ms; do java -jar $JAR $DB --bench.scenario=hot-key --bench.workers=1 $LW=$w; done
for w in 0 100ms; do java -jar $JAR $DB --bench.scenario=throughput $LW=$w; done
for w in 0 100ms; do java -jar $JAR $DB --bench.scenario=hot-key --bench.executor=virtual $LW=$w; done
for w in 0 100ms; do java -jar $JAR $DB --bench.scenario=hot-key --bench.lock=redis --bench.redis-uri=redis://127.0.0.1:56379 $LW=$w; done
MIX="--bench.scenario=hot-key --bench.lock-keys=64 --bench.slow-key-share=0.02 --bench.slow-key-work-time=200ms"
for w in 0 100ms 500ms; do java -jar $JAR $DB $MIX $LW=$w; done
for w in 0 100ms; do java -jar $JAR $DB --bench.scenario=crash $LW=$w; done
```

`hot-key` is the shipped preset: 5 000 events on 8 keys, 5 ms of
work, 3 workers × 3 handler threads, lease locker. The mixed cell keeps
the preset but spreads the events over 64 keys and routes 2 % of them
(≈100 events) to one extra key `key-slow` whose handler takes 200 ms —
a serial floor of 20 s for that key alone.

## Results

Every cell passed its invariants: `lost = 0`, no unexplained
duplicate, `lockOverlaps = 0` wherever a locker was on, storage clean,
no Redis key left behind. Busy hits are the `release` statement count
(one per busy hit); `writes/event` is `n_tup_ins + n_tup_upd +
n_tup_del` over the `event_outboxer` schema.

### `hot-key`, lease locker, 3 workers

| `lock-wait` | handled/s | e2e p50 | e2e p95 | e2e p99 | e2e max | writes/event | statements/event | busy hits |
|---|---|---|---|---|---|---|---|---|
| 0 | 256 | 4 560 ms | 13 069 ms | 15 121 ms | 19 166 ms | 6.65 | 6.89 | 3 937 |
| 20 ms | 253 | 5 520 ms | 9 588 ms | 12 661 ms | 18 156 ms | 5.16 | 5.52 | 190 |
| **100 ms** | **337** | 4 728 ms | **8 251 ms** | **8 560 ms** | **8 660 ms** | **5.04** | **5.39** | **4** |
| 500 ms | 294 | 5 536 ms | 9 843 ms | 10 217 ms | 10 310 ms | 5.08 | 5.47 | 4 |

### `hot-key`, lease locker, one worker

| `lock-wait` | handled/s | e2e p50 | e2e p95 | writes/event | busy hits |
|---|---|---|---|---|---|
| 0 | 137 | 14 594 ms | 27 506 ms | 5.00 | 2 |
| 100 ms | 141 | 14 561 ms | 27 233 ms | 5.00 | 2 |

### `throughput`, no lock key

| `lock-wait` | publish/s | handled/s | e2e p50 | e2e p95 | writes/event | statements/event |
|---|---|---|---|---|---|---|
| 0 | 1 505 | 1 496 | 51 ms | 92 ms | 3.00 | 1.37 |
| 100 ms | 1 316 | 1 311 | 51 ms | 94 ms | 3.00 | 1.39 |

### `hot-key`, Redis locker

| `lock-wait` | handled/s | e2e p50 | e2e p95 | e2e max | PG writes/event | PG statements/event | Redis cmds/event | busy hits |
|---|---|---|---|---|---|---|---|---|
| 0 | 315 | 1 441 ms | 7 225 ms | 14 686 ms | 5.38 | 4.04 | 5.19 | 5 935 |
| 100 ms | **412** | 2 945 ms | **4 824 ms** | **8 201 ms** | **3.01** | **2.85** | 5.32 | **11** |

### Mixed: 64 keys, 2 % of events on a 200 ms key

| `lock-wait` | handled/s | drain | e2e p50 | e2e p95 | e2e p99 | e2e max | writes/event | busy hits |
|---|---|---|---|---|---|---|---|---|
| 0 | 80 | 62.7 s | 3 266 ms | 5 866 ms | 12 684 ms | 57 480 ms | 5.48 | 1 127 |
| 100 ms | 180 | 27.8 s | 3 862 ms | 7 141 ms | 7 481 ms | 23 908 ms | 5.09 | 197 |
| 500 ms | 219 | 22.8 s | 5 220 ms | 10 053 ms | 10 424 ms | 22 293 ms | 5.04 | 61 |

### `crash`, lease locker, forked fleet (two of three workers killed and respawned)

| `lock-wait` | handled/s | e2e p50 | e2e p95 | e2e max | writes/event | busy hits | retries | duplicates |
|---|---|---|---|---|---|---|---|---|
| 0 | 175 | 13 808 ms | 25 347 ms | 32 241 ms | 6.44 | 3 288 | 203 | 2 (attributable) |
| 100 ms | 190 | 15 988 ms | 26 754 ms | 29 534 ms | 5.34 | 507 | 203 | 1 (attributable) |

### `hot-key`, virtual executor, in-flight budget uncapped (103 per type)

| `lock-wait` | handled/s | e2e p50 | e2e p95 | e2e max | writes/event | statements/event | busy hits | lease probes |
|---|---|---|---|---|---|---|---|---|
| 0 | 89 | 16 604 ms | 36 437 ms | 53 492 ms | 21.08 | 20.03 | 38 490 | ≈38 000 |
| 100 ms | 53 | 44 017 ms | 87 232 ms | 93 172 ms | 20.87 | 35.50 | 37 230 | ≈117 000 |

## Findings

### 1. 100 ms removes the busy round trip and a third of the drain time

On 5 ms holds the wait at 100 ms turns 0.79 busy hits per event into
none: the lease locker lands on its floor of 5.00 row writes per event
(the lease's insert and delete; 5.04 measured), the Redis locker on the
engine's floor of 3.00 (3.01), and handled/s rises 256 → 337 with the
lease and 315 → 412 with Redis. The tail collapses with it: e2e p99
15.1 s → 8.6 s, max 19.2 s → 8.7 s. Twenty milliseconds already
removes 95 % of the busy hits but recovers none of the throughput —
the 190 remaining releases still go to the back of the backlog, and
each one is a multi-second delay. Five hundred milliseconds gains
nothing over 100 ms on holds this short and costs a little (294/s):
one run, so within noise, but there is no reason to wait longer than
the hold.

The residual gap to the `noop` baseline (612/s in the first session)
and to the per-key serial ideal is the locker's own round trips on
this commit-bound host: acquire and release are one fsync each next to
the 5 ms of work, so each key can turn over roughly every 15–20 ms.

### 2. Collisions are cross-JVM

One worker with three threads produces two busy hits in 5 000 events,
wait or no wait: consecutive claims of one poller carry consecutive
sequence numbers and therefore different keys. The "local key
affinity" alternative in the ADR would have had nothing to do; the
waiting has to happen against other JVMs' holds, which is what the
backend-level wait does.

### 3. Invisible without a key

`throughput` does not declare a lock key and does not touch the
locker; both cells show 3.00 writes and ~1.4 statements per event with
the same latency shape. The handled/s difference is the publisher's
(1 505 vs 1 316/s — the fleet kept pace with it in both runs).

### 4. The mixed workload rewards the wait instead of punishing it

The ADR's Negative section feared that a non-zero default would cost
throughput next to a slow hot key. The measured shape is the opposite:
`lock-wait: 0` is the worst cell by far (80/s, a 57 s maximum),
because every collision on the 200 ms key sends the event back with a
1 s delay to the back of a backlog that the cool keys keep refilling;
the slow key's ~100 events take 63 s instead of their 20 s floor.
At 100 ms the waiters mostly still miss the 200 ms hold, but the ones
that arrive in its last 100 ms chain onto it, and the drain takes
27.8 s. At 500 ms they always chain (22.8 s, close to the floor).

The cost the ADR predicted is real, just smaller than the benefit: the
cool keys' median latency goes 3.3 s → 3.9 s at 100 ms and → 5.2 s at
500 ms, because handler threads park behind the slow holder. That is
the trade-off the default strikes, and 100 ms strikes it on the right
side: most of the drain-time gain, a fraction of the latency cost. A
type known to hold its keys for hundreds of milliseconds can raise its
own `lock-wait`, at that type's latency.

### 5. After a crash the wait degrades to the release path

Survivors that hit the dead holder's lease give up after 100 ms and
release as before: busy cycles fall from 3 288 to 507 (0.66 → 0.10 per
event), row writes from 6.44 to 5.34, drain rate 175 → 190/s, and the
duplicates stay the attributable ones the kill causes. Nothing stalls
until `lock-ttl`, which is what the bound is for.

### 6. An uncapped virtual executor on a hot key gets worse

The executor session already showed that `handler-executor.type:
virtual` dispatches every claimed event of the 103-slot in-flight
budget at once, so on `hot-key` some 5 000 dispatches attempt eight
keys and 7.7 busy hits per event follow. With the wait those
dispatches park and poll instead: 117 000 lease probes (35 statements
per event) that cost more on this host than the round trips they
save, and the cell slows from 89 to 53/s while busy hits stay at 7.4
per event — a 100 ms wait is nowhere near the queue that hundreds of
waiters per key form. The rule from the executor session stands and is
now documented next to `lock-wait`: cap the in-flight budget near the
live key count on keyed virtual types. A wake-up instead of polling
(Redis pub/sub, or `LISTEN/NOTIFY` on the lease table) would turn the
probes into parks and is the natural follow-up.

## Decision taken

`lock-wait` defaults to **100 ms** (`EventTypeConfig.defaults()`,
`event-outboxer.event-types.defaults.lock-wait`); `0` restores the
one-attempt flow of ADR-0012. ADR-0035 is accepted with this session as
its evidence; ADR-0012 is amended.

## Follow-ups

- Wake-up instead of polling for the lease and Redis lockers.
- The harness could count `onLockAcquired.waited` directly instead of
  deriving busy hits from statement counts.
