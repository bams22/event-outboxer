# 2026-09-04 (second session) — lease vs Redis locker, and what each costs the database

**Qualifies for README numbers: no.** Same laptop as the
[first session](2026-09-04-laptop-first-run.md); PostgreSQL and Redis
both ran in standalone containers on it. Read the variants against
each other, not as absolute numbers.

## Setup

Everything as in the first session (Intel Core i7-13700H, Temurin
25.0.2, PostgreSQL 15.17 in a standalone `postgres:15` container with
`shared_buffers=1GB`, `fsync=on`), plus Redis 7.4.7 in a standalone
`redis:7-alpine` container on the same host. Harness at the commit
that adds `--bench.lock=redis` and the Redis probe; in-process fleet,
steady state, one run per variant, no warm-up.

```bash
docker run -d --name outboxer-bench-redis -p 127.0.0.1:56379:6379 redis:7-alpine
DB="... --bench.redis-uri=redis://127.0.0.1:56379"
java -jar $JAR $DB --bench.scenario=throughput --bench.lock-keys=100000 --bench.lock=noop
java -jar $JAR $DB --bench.scenario=throughput --bench.lock-keys=100000 --bench.lock=postgres-lease
java -jar $JAR $DB --bench.scenario=throughput --bench.lock-keys=100000 --bench.lock=redis
java -jar $JAR $DB --bench.scenario=hot-key
java -jar $JAR $DB --bench.scenario=hot-key --bench.lock=redis
```

`--bench.lock-keys=100000` on 20 000 events gives every event its own
key: the locker does its full acquire/release work with no contention
at all — the cost of *having* a locker. `hot-key` (5 000 events on 8
keys, 5 ms of work) is the cost of contention.

## Results

Every run passed: `lost = 0`, no duplicate, no overlap under a
locker, storage clean, no Redis lock key left behind.

| Variant | handled/s | e2e p50 | e2e p95 | PG writes/event | Redis cmds/event |
|---|---|---|---|---|---|
| unique keys, `noop` | 1 241 | 243 ms | 2 923 ms | **3.00** | — |
| unique keys, lease | 1 499 | 195 ms | 686 ms | **5.00** | — |
| unique keys, redis | 1 508 | 55 ms | 96 ms | **3.00** | **4.00** |
| hot-key, lease | 249 | 4 278 ms | 12 693 ms | 6.61 | — |
| hot-key, redis | 336 | 1 537 ms | 6 853 ms | 5.52 | 5.26 |

PG writes = `n_tup_ins + n_tup_upd + n_tup_del` over the
`event_outboxer` schema per event. Redis commands =
`total_commands_processed` from `INFO stats` per event; commands
issued inside a Lua script count individually.

## Findings

### 1. The lease locker costs PostgreSQL exactly two row writes per locked event

Unique keys: `5.00` with the lease locker against `3.00` without a
locker and `3.00` with Redis. The two extra rows are the lease's
`INSERT` and its `DELETE` (`ins 40 004` / `del 40 004` against
`20 004` / `20 004`). With the Redis locker the PostgreSQL side is
indistinguishable from no locker at all, and the lock traffic shows up
on Redis as `4.00` commands per event: `SET NX PX` on acquire, and on
release the `EVAL` plus the `GET` and `DEL` it runs.

For a shared database this is the number to take to the DBA: with a
lease locker every keyed event is five row writes instead of three, a
67 % increase in write rows on the outbox schema; with Redis it stays
at three and the locker's load moves to a store built for it.

### 2. Without contention, throughput is the same either way on this host

Lease `1 499/s`, Redis `1 508/s`: the fleet kept pace with the
publisher in both runs (handled/s equals publish/s), so the locker was
not the bottleneck. The `noop` run being slower (`1 241/s`, p95 ~3 s)
is the run-to-run noise of a shared laptop, not a property of not
locking — the same preset without keys did 1 373–1 646/s in the first
session.

End-to-end latency did move: p50 195 ms and p95 686 ms with the
lease against 55 ms and 96 ms with Redis. Two extra PostgreSQL round
trips per event on twelve handler threads at 1 500 events/s is enough
extra thread occupancy to let the queue build a little; Redis's
sub-millisecond round trips do not. A real signal, but one run each —
treat the size of the gap as indicative.

### 3. Under contention Redis is faster, and both are far from the ideal

Hot keys: Redis `336/s` and p50 1.5 s against the lease's `249/s` and
p50 4.3 s. Neither is close to the ~1 600/s per-key serial ideal or
even to the incorrect `noop` baseline (612/s in the first session).
The mechanism is the one [ADR-0035](../adr/0035-bounded-lock-wait.md)
describes — claim, find the key busy, release to the back of the
backlog, claim again — and it is backend-independent. Redis only
makes each failed attempt cheaper (a `SET NX` round trip instead of
an upsert with a tuple lock), so a thread gets back to the queue
sooner and catches the free window more often.

### 4. A correction: busy hits are half what the first report said

The Redis counters make the accounting exact. A busy hit costs
PostgreSQL **two** updates — the release back to `PENDING` and the
later re-claim to `PROCESSING` — so the number of busy hits is half
the extra updates, not equal to them:

| Run | extra updates | busy hits | per event |
|---|---|---|---|
| hot-key, lease (this session) | 7 676 | 3 838 | 0.77 |
| hot-key, redis (this session) | 12 587 | 6 293 | 1.26 |
| hot-key, lease (first session) | 9 145 | 4 572 | 0.91 |

For the Redis run the cross-check closes: `26 290` commands = 5 000
successful acquire/release cycles × 4 + 6 290 busy `SET NX` calls. The
first session's report and ADR-0035 said "1.6–1.8 busy hits per
event"; the correct figure is 0.8–0.9, and both documents are amended.
The conclusions stand — the cost per hit (two row writes and a trip to
the back of the backlog) is what makes the path slow — but the ratio
is worth getting right before it is quoted. Redis showing *more*
hits per event than the lease while being faster is the same effect
seen from the other side: cheaper failed attempts mean more attempts.

## Console summaries

```
unique keys, noop
publish      20000 in 13.2s = 1512/s   p50 5.8ms p95 6.2ms p99 6.4ms max 38.6ms
processing   drained 20000/20000 in 16.1s = 1241/s   e2e p50 243ms p95 2923ms p99 3375ms max 3709ms   handlings=20000 retries=0
database     60034 row writes (ins 20014, upd 20016, del 20004) = 3.00/event

unique keys, postgres-lease
publish      20000 in 12.8s = 1564/s   p50 5.0ms p95 6.3ms p99 8.5ms max 49.3ms
processing   drained 20000/20000 in 13.3s = 1499/s   e2e p50 195ms p95 686ms p99 787ms max 882ms   handlings=20000 retries=0
database     100020 row writes (ins 40004, upd 20012, del 40004) = 5.00/event

unique keys, redis
publish      20000 in 13.2s = 1514/s   p50 5.8ms p95 6.2ms p99 6.8ms max 30.7ms
processing   drained 20000/20000 in 13.3s = 1508/s   e2e p50 55ms p95 96ms p99 175ms max 247ms   handlings=20000 retries=0
database     60020 row writes (ins 20004, upd 20012, del 20004) = 3.00/event
redis        80004 commands = 4.00/event   lock keys left=0   (7.4.7, external)

hot-key, postgres-lease
publish      5000 in 5.9s = 846/s   p50 4.1ms p95 6.2ms p99 7.0ms max 17.1ms
processing   drained 5000/5000 in 20.1s = 249/s   e2e p50 4278ms p95 12693ms p99 14562ms max 17694ms   handlings=5000 retries=0
database     33040 row writes (ins 10182, upd 12676, del 10182) = 6.61/event

hot-key, redis
publish      5000 in 5.7s = 882/s   p50 4.0ms p95 6.1ms p99 7.7ms max 31.7ms
processing   drained 5000/5000 in 14.9s = 336/s   e2e p50 1537ms p95 6853ms p99 8933ms max 12015ms   handlings=5000 retries=0
database     27595 row writes (ins 5004, upd 17587, del 5004) = 5.52/event
redis        26290 commands = 5.26/event   lock keys left=0   (7.4.7, external)
```

All five: `invariants lost=0 duplicates=0 unexpected=0 lockOverlaps=0 storage: events=0 locks=0`, `RESULT PASS`.

## Next

1. The ADR-0035 validation plan now has a second backend to run
   against: a bounded wait should narrow the lease/Redis gap under
   contention, because it removes the failed attempts both pay for.
2. Repeat the unique-keys pair with a warm-up and three runs each
   before quoting the latency gap in finding 2.
