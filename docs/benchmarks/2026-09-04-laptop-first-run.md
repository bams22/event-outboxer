# 2026-09-04 — first numbers on a developer laptop

**Qualifies for README numbers: no.** Publisher, workers and PostgreSQL
all ran on one laptop. The figures are good for comparing variants
against each other and for spotting where the engine's time goes; they
are not what a service sees on real infrastructure.

## Setup

| | |
|---|---|
| Host | laptop, Intel Core i7-13700H (20 hardware threads), 30 GB RAM, Linux 7.0 |
| JVM | Temurin 25.0.2; driver heap 7.9 GB (default), forked workers `-Xmx1g` |
| PostgreSQL | 15.17, `postgres:15` image in a standalone Docker container on the same host, `shared_buffers=1GB`, `max_connections=200`, `fsync=on`, data directory on the container's overlay filesystem. Not managed by the harness (`--bench.jdbc-url`), one container for the whole session |
| Harness | `event-outboxer-benchmark` at commit `6c02559` (0.8.0-SNAPSHOT working tree), exec jar built with `-P bench` |
| Fleet | in-process (contexts in the driver JVM) unless stated; one run with the forked fleet |
| Ledger | in-memory for in-process runs, `bench.handled` for the forked run |
| Method | one run per variant, no warm-up, steady state (publishing and handling overlap); harness defaults `poll-min-interval=100ms`, `poll-max-interval=1s` (starter defaults are 500 ms / 10 s) |

```bash
./mvnw -B -ntp -DskipTests install
./mvnw -B -ntp -pl event-outboxer-benchmark -P bench -DskipTests package
docker run -d --name outboxer-bench-pg -e POSTGRES_USER=bench -e POSTGRES_PASSWORD=bench \
  -e POSTGRES_DB=bench -p 127.0.0.1:55432:5432 postgres:15 -c shared_buffers=1GB -c max_connections=200
JAR=event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar
DB="--bench.jdbc-url=jdbc:postgresql://127.0.0.1:55432/bench --bench.jdbc-user=bench --bench.jdbc-password=bench"
java -jar $JAR $DB --bench.scenario=throughput
java -jar $JAR $DB --bench.scenario=throughput --bench.fleet=forked
java -jar $JAR $DB --bench.scenario=throughput --bench.finalize-batching=false
java -jar $JAR $DB --bench.scenario=hot-key
java -jar $JAR $DB --bench.scenario=hot-key --bench.lock=noop
java -jar $JAR $DB --bench.scenario=hot-key --bench.worker-prop.event-outboxer.dispatcher.lock-busy-retry-delay=50ms
java -jar $JAR $DB --bench.scenario=hot-key --bench.poll-max-interval=100ms \
  --bench.worker-prop.event-outboxer.dispatcher.lock-busy-retry-delay=50ms
```

Presets as shipped at that commit: `throughput` = 20 000 events, 4
event types, 3 workers, `handler-pool-size 4`, `claim-batch-size 50`,
8 publisher threads, no lock key, no simulated work; `hot-key` = 5 000
events, 1 type, 3 workers, `handler-pool-size 3`, `claim-batch-size 10`,
8 lock keys, 5 ms of simulated work per handling, lease locker.

## Results

Every run passed its invariants: `lost = 0`, no duplicate, nothing
unexpected, storage clean after the graceful stop. Overlaps are graded
only when a locker is on.

| Variant | publish/s | handled/s | e2e p50 | e2e p95 | e2e p99 | writes/event | overlaps |
|---|---|---|---|---|---|---|---|
| throughput, in-process | 1 875 | 1 646 | 139 ms | 2 954 ms | 3 142 ms | 3.00 | 0 |
| throughput, forked fleet | 1 382 | 1 373 | 58 ms | 108 ms | 130 ms | 3.00 | 0 |
| throughput, `finalize-batching=false` | 1 473 | 1 467 | 45 ms | 87 ms | 174 ms | 3.00 | 0 |
| hot-key, lease locker | 1 014 | **213** | 4 608 ms | 14 883 ms | 17 290 ms | 6.88 | 0 (graded) |
| hot-key, `lock=noop` (baseline) | 644 | 612 | 212 ms | 445 ms | 643 ms | 3.00 | 1 140 (informational) |
| hot-key, lease, `lock-busy-retry-delay=50ms` | 611 | 255 | 5 088 ms | 13 697 ms | 16 242 ms | 6.68 | 0 (graded) |
| hot-key, lease, busy 50 ms + `poll-max-interval=100ms` | 853 | 298 | 4 327 ms | 12 405 ms | 14 280 ms | 6.72 | 0 (graded) |

`e2e` = publish call start to first successful handling end.
`writes/event` = `n_tup_ins + n_tup_upd + n_tup_del` over the
`event_outboxer` schema, sampled after every connection closed,
divided by events; heartbeats and worker registration included.

## Findings

### 1. Three row writes per event, and nothing else

All three `throughput` variants land on exactly `3.00`: one insert
(publish), one update (claim), one delete (finalize). Over 20 000
events the update column shows 12 extra rows — heartbeats and worker
registration. There is no claim/release churn under load, which is the
claim the capacity-coupled polling amendment of ADR-0004 makes
("~2 wasted hot-table writes per event" removed). Measured, not
derived, for the first time.

### 2. The throughput ceiling on this host is publisher-bound

The fleet (3 workers × 4 types × 4 threads, no simulated work) handled
1 370–1 650 events/s. In two of the three variants handled/s equals
publish/s to within 1 %, and end-to-end latency stays around 50–100 ms:
the fleet kept pace with the publisher, which shares the same CPU and
database. In the in-process variant the publisher was faster
(1 875/s) than the fleet (1 646/s), so a backlog built up during the
10.7 s publish phase and the p95 rose to about 3 s — the signature of
sustained overload, not of a latency problem.

Consequences for reading the table: the in-process vs forked
difference (1 646 vs 1 373) mostly measures how fast the publisher
could push while three extra JVMs competed for the CPU, and the
`finalize-batching` on/off pair (1 646 vs 1 467) is within that same
publisher noise. **Group commit's effect is not measurable in steady
state on one host.** The `backlog` preset (publish everything, then
start the fleet) isolates the drain rate and is the right tool; it was
not run in this session.

### 3. Hot keys under a locker: the first scene where the library looks bad

With 8 lock keys and 5 ms of work per event, the per-key serial ideal
is about 1 600 events/s (8 keys × 200/s). The lease locker delivered
213/s; the no-locker baseline 612/s. So the locker costs roughly 3×
against the baseline and 7× against the ideal.

The write counters say where the time goes. Per event the lease adds
one insert and one delete (`~5 000` each), and the update column
carries about 8 000–9 000 rows beyond the 5 000 claims: each is a
*lock-busy release* — a worker claimed an event, found its key held by
another handling, wrote the event back to `PENDING` with a retry
delay, and moved on. That is 1.6–1.8 busy hits per event, two row
writes each.

Two hypotheses about the timing knobs were tested and both fell:

- `dispatcher.lock-busy-retry-delay` from the default 1 s down to
  50 ms: 255/s instead of 213/s, within run-to-run noise.
- additionally `poll-max-interval` from 1 s down to 100 ms, so the
  poller's adaptive back-off cannot delay the re-claim: 298/s.

The busy-hit count did not move either (updates 13 038 and 13 228 vs
14 145). The cause is therefore structural, not a timer: the engine
claims events without knowing their lock keys (the key lives in the
handler, ADR-0012), so three workers keep claiming events whose keys
are currently held, releasing them and claiming them again. The root
cause of the remaining gap to the ideal — why a released event waits
seconds rather than one poll interval — was not found in this session
and is an open question for an issue.

The `noop` baseline is part of the same picture: 1 140 overlapping
handlings on the same key. Without a locker the hot-key scenario is
fast and wrong.

### 4. Noise on a shared host

Publish latency p50 moved between 3.2 ms and 5.9 ms across otherwise
identical runs, and single outliers of 0.3–2 s appear in the publish
`max` column (checkpoints, JIT, the other JVMs). Differences below
about 20 % between two runs of this session are not signal.

## Console summaries

Verbatim harness output; the JSON reports embed the effective scenario
and were kept by the author.

```
throughput, in-process
publish      20000 in 10.7s = 1875/s   p50 3.2ms p95 5.2ms p99 6.7ms max 2089.9ms
processing   drained 20000/20000 in 12.2s = 1646/s   e2e p50 139ms p95 2954ms p99 3142ms max 3368ms   handlings=20000 retries=0
database     60030 row writes (ins 20014, upd 20012, del 20004) = 3.00/event
invariants   lost=0 duplicates=0 (attributable 0, unexplained 0) unexpected=0 lockOverlaps=0 (informational, no locker)   storage: events=0 locks=0

throughput, forked fleet
publish      20000 in 14.5s = 1382/s   p50 5.9ms p95 6.6ms p99 9.1ms max 41.6ms
processing   drained 20000/20000 in 14.6s = 1373/s   e2e p50 58ms p95 108ms p99 130ms max 161ms   handlings=20000 retries=0
database     60015 row writes (ins 20004, upd 20010, del 20001) = 3.00/event
invariants   lost=0 duplicates=0 (attributable 0, unexplained 0) unexpected=0 lockOverlaps=0 (informational, no locker)   storage: events=0 locks=0

throughput, finalize-batching=false
publish      20000 in 13.6s = 1473/s   p50 5.8ms p95 6.3ms p99 7.0ms max 33.3ms
processing   drained 20000/20000 in 13.6s = 1467/s   e2e p50 45ms p95 87ms p99 174ms max 204ms   handlings=20000 retries=0
database     60020 row writes (ins 20004, upd 20012, del 20004) = 3.00/event
invariants   lost=0 duplicates=0 (attributable 0, unexplained 0) unexpected=0 lockOverlaps=0 (informational, no locker)   storage: events=0 locks=0

hot-key, lease locker
publish      5000 in 4.9s = 1014/s   p50 4.0ms p95 5.9ms p99 8.1ms max 23.9ms
processing   drained 5000/5000 in 23.5s = 213/s   e2e p50 4608ms p95 14883ms p99 17290ms max 21151ms   handlings=5000 retries=0
database     34408 row writes (ins 10130, upd 14145, del 10133) = 6.88/event
invariants   lost=0 duplicates=0 (attributable 0, unexplained 0) unexpected=0 lockOverlaps=0 (graded)   storage: events=0 locks=0

hot-key, lock=noop
publish      5000 in 7.8s = 644/s   p50 5.9ms p95 6.3ms p99 8.9ms max 308.8ms
processing   drained 5000/5000 in 8.2s = 612/s   e2e p50 212ms p95 445ms p99 643ms max 688ms   handlings=5000 retries=0
database     15016 row writes (ins 5004, upd 5008, del 5004) = 3.00/event
invariants   lost=0 duplicates=0 (attributable 0, unexplained 0) unexpected=0 lockOverlaps=1140 (informational, no locker)   storage: events=0 locks=0

hot-key, lease, lock-busy-retry-delay=50ms
publish      5000 in 8.2s = 611/s   p50 4.0ms p95 6.1ms p99 16.0ms max 1138.8ms
processing   drained 5000/5000 in 19.6s = 255/s   e2e p50 5088ms p95 13697ms p99 16242ms max 18704ms   handlings=5000 retries=0
database     33396 row writes (ins 10179, upd 13038, del 10179) = 6.68/event
invariants   lost=0 duplicates=0 (attributable 0, unexplained 0) unexpected=0 lockOverlaps=0 (graded)   storage: events=0 locks=0

hot-key, lease, busy 50ms + poll-max-interval=100ms
publish      5000 in 5.9s = 853/s   p50 4.1ms p95 6.1ms p99 6.5ms max 40.0ms
processing   drained 5000/5000 in 16.8s = 298/s   e2e p50 4327ms p95 12405ms p99 14280ms max 16300ms   handlings=5000 retries=0
database     33600 row writes (ins 10191, upd 13228, del 10181) = 6.72/event
invariants   lost=0 duplicates=0 (attributable 0, unexplained 0) unexpected=0 lockOverlaps=0 (graded)   storage: events=0 locks=0
```

## Next

1. `backlog` preset with `finalize-batching` on and off on the same
   database: the only way to measure group commit (ADR-0014 amendment)
   separately from the publisher.
2. The hot-key path: the "seconds before the next attempt" question
   was answered the same day — a busy-released event carries
   `run_at = now + delay` and the claim query orders by `run_at`, so it
   re-enters at the back of the backlog. The proposed fix is a bounded
   wait for the lock, [ADR-0035](../adr/0035-bounded-lock-wait.md)
   (proposed, implementation deferred), with its validation plan built
   on this preset.
3. A session on separate hardware with an external PostgreSQL before
   any number reaches the README.
