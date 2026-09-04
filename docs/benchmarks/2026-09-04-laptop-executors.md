# 2026-09-04 (ninth session) — `platform` vs `virtual` handler executor

**Qualifies for README numbers: no.** Same laptop as the
[first session](2026-09-04-laptop-first-run.md); PostgreSQL in a
standalone container on the same host. Read the cells against each
other.

## Question

`event-outboxer.handler-executor.type` has two values. What does
switching from `platform` to `virtual` change in throughput, database
cost and JVM footprint — and is the change a property of the thread
kind, or of something else the switch moves along with it?

## Setup

Intel Core i7-13700H (20 hardware threads, 30 GB), Temurin 25.0.2,
PostgreSQL 15.17 in a standalone `postgres:15` container
(`shared_buffers=1GB`, `max_connections=200`, `fsync=on`,
`synchronous_commit=on`, `pg_stat_statements` preloaded) on the same
host. Harness at the commit that adds the `concurrency` summary line;
in-process fleet, Jackson, 256 B payloads, one series, then the whole
series repeated (the second series is the one quoted, the first is
in "Repeat" below). No warm-up.

The `concurrency` line is new in this session: the peak number of
handlings whose ledger intervals overlap (fleet-wide and in the
busiest worker), the distinct handler threads seen, and the JVM's
peak platform thread count from `ThreadMXBean` — the driver's own
threads included, virtual threads excluded.

```bash
docker run -d --name outboxer-bench-pg -e POSTGRES_USER=bench -e POSTGRES_PASSWORD=bench \
  -e POSTGRES_DB=bench -p 127.0.0.1:55432:5432 postgres:15 -c shared_buffers=1GB -c max_connections=200 \
  -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=all
JAR=event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar
DB="--bench.jdbc-url=jdbc:postgresql://127.0.0.1:55432/bench --bench.jdbc-user=bench --bench.jdbc-password=bench"
BACKLOG="--bench.scenario=throughput --bench.workers-after-publish=true"   # 20 000 events, 3 workers x 4 types, batch 50
Q0="--bench.worker-prop.event-outboxer.event-types.defaults.handler-queue-capacity=0"
MINFREE="--bench.worker-prop.event-outboxer.event-types.defaults.claim-min-free=50"

# A. flip the switch on the preset as it is: pool 4, queue 100 (in-flight budget 104 per type)
java -jar $JAR $DB $BACKLOG --bench.executor=platform
java -jar $JAR $DB $BACKLOG --bench.executor=virtual
java -jar $JAR $DB $BACKLOG --bench.executor=platform --bench.handler-work-time=50ms
java -jar $JAR $DB $BACKLOG --bench.executor=virtual  --bench.handler-work-time=50ms
java -jar $JAR $DB $BACKLOG --bench.executor=platform $MINFREE                       # control: claim batching alone
# B. equal executing concurrency: 32 per type, no queue, 50 ms of blocking work
java -jar $JAR $DB $BACKLOG --bench.executor=platform --bench.handler-pool-size=32 --bench.handler-work-time=50ms $Q0
java -jar $JAR $DB $BACKLOG --bench.executor=virtual  --bench.handler-pool-size=32 --bench.handler-work-time=50ms $Q0
# C. many blocking handlers: one worker, 500 per type, no queue, 200 ms of blocking work
java -jar $JAR $DB $BACKLOG --bench.workers=1 --bench.executor=platform --bench.handler-pool-size=500 --bench.handler-work-time=200ms $Q0 $MINFREE
java -jar $JAR $DB $BACKLOG --bench.workers=1 --bench.executor=virtual  --bench.handler-pool-size=500 --bench.handler-work-time=200ms $Q0 $MINFREE
# H. hot keys under the lease locker: the preset (pool 3, queue 100, 8 keys, 5 ms, steady state)
java -jar $JAR $DB --bench.scenario=hot-key --bench.executor=platform
java -jar $JAR $DB --bench.scenario=hot-key --bench.executor=virtual
```

"Blocking work" is `Thread.sleep` in the handler. A virtual thread
unmounts on sleep exactly as it does on a socket read or a JDBC call
after JEP 491, so this is the intended case for virtual threads, not
a corner of it; nothing here measures pinning.

The backlog cells publish everything first, so their end-to-end
percentiles include the wait for the fleet to start and are not
compared. Drain rate is the number.

## Results

Every run passed: `lost = 0`, no duplicate, no overlap under the
locker, storage clean.

| Cell | executor | handled/s | peak in-flight (busiest worker) | JVM peak platform threads | statements/event | claim rows/call | finalize rows/batch | PG writes/event |
|---|---|---|---|---|---|---|---|---|
| A. preset, 0 ms | platform | 4 607 | 16 (16) | 88 | 1.48 | 2.6 | 10.5 | 3.00 |
| A. preset, 0 ms | **virtual** | **23 054** | 20 (20) | 63 | 1.04 | 30.6 | 119.7 | 3.00 |
| A. preset, 0 ms, `claim-min-free: 50` | platform | 4 478 | 16 (16) | 88 | 1.12 | 47.2 | 10.4 | 3.00 |
| A. preset, 50 ms | platform | 783 | 48 (16) | 87 | 2.40 | 1.2 | 3.2 (+8 092 single) | 3.00 |
| A. preset, 50 ms | **virtual** | **12 166** | **1 216 (416)** | 63 | 1.11 | 12.1 | 42.0 | 3.00 |
| B. 32 per type, no queue, 50 ms | platform | 5 001 | 383 (128) | **424** | 1.42 | 3.1 | 12.0 | 3.00 |
| B. 32 per type, no queue, 50 ms | virtual | 4 983 | 370 (128) | **63** | 1.36 | 3.6 | 13.4 | 3.00 |
| C. 500 per type, 1 worker, 200 ms | platform | 7 545 | 2 000 (2 000) | **2 022** | 1.03 | 49.0 | 81.6 | 3.00 |
| C. 500 per type, 1 worker, 200 ms | virtual | 7 603 | 2 000 (2 000) | **45** | 1.03 | 49.5 | 94.7 | 3.00 |
| H. hot-key, lease | platform | **218** | 6 (3) | 49 | 6.72 | 1.3 | — | **6.49** |
| H. hot-key, lease | virtual | **103** | 8 (8) | 63 | **20.19** | 9.5 | — | **21.23** |

Busy hits on the hot-key cells, from the `release` statement count:
3 564 (0.71 per event) on platform, 38 893 (7.8 per event) on virtual.
`handler threads` reads 48 = 3 workers × 4 types × 4 for the platform
preset, 384 and 2 000 for B and C, and 20 000 — one per handling —
for every virtual cell.

## Findings

### 1. At equal concurrency the executors are equal; the difference is threads

Cells B and C hold the executing concurrency fixed (no queue, so the
in-flight budget is the pool size, and the measured peak confirms
it: 383/370 and 2 000/2 000). Throughput, statements per event, claim
and finalize batch sizes and row writes are the same within
run-to-run noise. What differs is the `JVM peak platform threads`
column: 424 against 63 for 384 concurrent handlers, 2 022 against 45
for 2 000. That is the whole of what "virtual threads" buys in this
engine — the same concurrency without the threads — and it is not
nothing: 2 000 platform threads is a JVM most operators would not
run, 2 000 virtual ones is unremarkable.

### 2. The switch changes how many handlers run, not only what runs them

The platform executor runs `handler-pool-size` dispatches of a type
and queues the rest of the in-flight budget; the virtual executor has
no queue, so every claimed event runs the moment it is claimed. On the
preset that is 4 executing against 104. The measurement shows it
directly: peak in-flight 48 (16 per worker) on platform with 50 ms of
work, 1 216 (416 per worker = 4 types × 104) on virtual. Throughput
follows: 783/s, the pool's ceiling (12 pools × 4 threads / 50 ms =
960/s minus claim overhead), against 12 166/s, where the database is
the limit.

So "virtual is 15× faster" in cell A is true and is not a property
of virtual threads. It is the in-flight budget becoming executing
concurrency. A platform pool of 104 would do the same at the price
of 104 threads per type (finding 1).

### 3. Group commit batches follow executing concurrency, not claim size

The 0 ms cell is the interesting one: the handler does nothing, so
concurrency should not matter — and yet virtual drains 5× faster
(23 054/s against 4 607/s) at a third fewer statements. The
`statements` line explains it. With 104 dispatches completing at
once, the group-commit flusher finds 120 finalizes queued per flush
and the poller refills 30 rows per claim; with 4 threads it finds 10
(the number four threads complete during one ~5 ms commit) and the
poller, woken at every completion, claims 2.6.

The control cell separates the two: `claim-min-free: 50` on platform
brings the claim to 47 rows per call and statements to 1.12 per
event — and throughput does not move (4 478/s). The finalize batch
stays at 10 rows because only four handlers exist to complete
between flushes. The drain of a fast handler on commit-bound storage
is bounded by flushes per second × rows per flush, and rows per flush
is set by how many handlers finish together. That is what the virtual
executor changes.

### 4. Under contention the same multiplication is a 2× loss

The lock is taken inside `dispatch`, on the executor thread, after
the claim. With three platform threads per worker, three dispatches
of a type attempt the eight keys at a time; with the virtual executor
all 103 in-flight dispatches attempt them at once, eight succeed and
the rest take the busy path — a `release` back to `PENDING`, a later
re-claim, and the lease upsert that found the key taken. Busy hits go
from 0.71 to 7.8 per event, row writes from 6.49 to 21.23, WAL from
2.5 to 9.0 KB per event, statements from 6.7 to 20.2, and the drain
from 218/s to 103/s: the database spends its commit budget on
failed attempts. This is [ADR-0035](../adr/0035-bounded-lock-wait.md)'s
mechanism amplified 34× (103 dispatches attempting the keys at a time instead of 3), and a fifth validation cell for it.

### 5. What to do with the switch

Do not flip it; configure it. `virtual` with `handler-pool-size` set
to the concurrency the handler's downstream and the connection pool
can take and `handler-queue-capacity: 0` gives the same throughput
as a platform pool of that size with a fraction of the threads
(finding 1) and, for fast handlers, the group-commit batches that
come with running a whole claim at once (finding 3). For keyed types
keep the concurrency near the number of keys live at once, or wait
for ADR-0035. [CONFIGURATION.md](../CONFIGURATION.md#event-outboxerhandler-executortype)
now says this.

## Repeat

The first series, same cells, same order, harness one commit earlier
(thread labels not yet unique, so its `handler threads` figures
undercount platform threads; everything else is comparable):

| Cell | platform | virtual |
|---|---|---|
| A. preset, 0 ms | 4 581/s | 21 393/s |
| A. preset, 50 ms | 805/s | 12 442/s (peak in-flight 1 246) |
| B. 32 per type, 50 ms | 4 914/s (424 threads) | 5 038/s (63 threads) |
| C. 500 per type, 200 ms | 7 543/s (2 022 threads) | 7 517/s (45 threads) |
| H. hot-key, lease | 237/s, 6.57 writes/event | 96/s, 21.08 writes/event |

## Console summaries (second series)

```
flip-0ms-platform
publish      20000 in 13.6s = 1466/s   p50 5.9ms p95 6.1ms p99 6.3ms max 43.3ms
processing   drained 20000/20000 in 4.3s = 4607/s   e2e p50 9266ms p95 13579ms p99 13864ms max 13881ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 16 (busiest worker 16)   handler threads 48   JVM peak platform threads 88
database     60015 row writes (ins 20004, upd 20007, del 20004) = 3.00/event   WAL 30.2MB = 1.5KB/event
statements   29652 calls = 1.48/event   claim 7722 calls x 2.6 rows   finalize batched 1912 calls x 10.5 rows, single 0   release 4   retry 0   other 14

flip-0ms-virtual
publish      20000 in 13.8s = 1454/s   p50 5.9ms p95 6.1ms p99 6.4ms max 63.0ms
processing   drained 20000/20000 in 867ms = 23054/s   e2e p50 7665ms p95 13418ms p99 13868ms max 14017ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 20 (busiest worker 20)   handler threads 20000   JVM peak platform threads 63
database     60014 row writes (ins 20004, upd 20006, del 20004) = 3.00/event   WAL 32.0MB = 1.6KB/event
statements   20841 calls = 1.04/event   claim 654 calls x 30.6 rows   finalize batched 167 calls x 119.7 rows, single 3   release 4   retry 0   other 13

flip-0ms-platform-minfree50
publish      20000 in 13.0s = 1539/s   p50 5.9ms p95 6.1ms p99 6.3ms max 24.2ms
processing   drained 20000/20000 in 4.5s = 4478/s   e2e p50 9510ms p95 13242ms p99 13329ms max 13388ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 16 (busiest worker 16)   handler threads 48   JVM peak platform threads 88
database     60015 row writes (ins 20004, upd 20007, del 20004) = 3.00/event   WAL 30.7MB = 1.6KB/event
statements   22386 calls = 1.12/event   claim 424 calls x 47.2 rows   finalize batched 1931 calls x 10.4 rows, single 13   release 4   retry 0   other 14

flip-50ms-platform
publish      20000 in 14.7s = 1361/s   p50 6.0ms p95 6.1ms p99 6.4ms max 45.5ms
processing   drained 20000/20000 in 25.6s = 783/s   e2e p50 20209ms p95 25054ms p99 25455ms max 25573ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 48 (busiest worker 16)   handler threads 48   JVM peak platform threads 87
database     60034 row writes (ins 20004, upd 20026, del 20004) = 3.00/event   WAL 29.3MB = 1.5KB/event
statements   47997 calls = 2.40/event   claim 16134 calls x 1.2 rows   finalize batched 3733 calls x 3.2 rows, single 8092   release 4   retry 0   other 34

flip-50ms-virtual
publish      20000 in 12.3s = 1623/s   p50 4.9ms p95 6.1ms p99 6.4ms max 25.1ms
processing   drained 20000/20000 in 1.6s = 12166/s   e2e p50 7272ms p95 12204ms p99 12521ms max 12656ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 1216 (busiest worker 416)   handler threads 20000   JVM peak platform threads 63
database     60014 row writes (ins 20004, upd 20006, del 20004) = 3.00/event   WAL 31.6MB = 1.6KB/event
statements   22234 calls = 1.11/event   claim 1655 calls x 12.1 rows   finalize batched 474 calls x 42.0 rows, single 88   release 4   retry 0   other 13

c32-50ms-platform
publish      20000 in 12.3s = 1626/s   p50 5.1ms p95 6.1ms p99 6.3ms max 61.0ms
processing   drained 20000/20000 in 4.0s = 5001/s   e2e p50 9078ms p95 12584ms p99 12641ms max 12669ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 383 (busiest worker 128)   handler threads 384   JVM peak platform threads 424
database     60015 row writes (ins 20004, upd 20007, del 20004) = 3.00/event   WAL 30.3MB = 1.6KB/event
statements   28308 calls = 1.42/event   claim 6418 calls x 3.1 rows   finalize batched 1647 calls x 12.0 rows, single 225   release 4   retry 0   other 14

c32-50ms-virtual
publish      20000 in 15.1s = 1328/s   p50 6.0ms p95 6.2ms p99 6.4ms max 26.8ms
processing   drained 20000/20000 in 4.0s = 4983/s   e2e p50 9747ms p95 14886ms p99 15268ms max 15358ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 370 (busiest worker 128)   handler threads 20000   JVM peak platform threads 63
database     60015 row writes (ins 20004, upd 20007, del 20004) = 3.00/event   WAL 30.2MB = 1.5KB/event
statements   27248 calls = 1.36/event   claim 5549 calls x 3.6 rows   finalize batched 1473 calls x 13.4 rows, single 208   release 4   retry 0   other 14

c500-200ms-platform
publish      20000 in 13.7s = 1456/s   p50 5.9ms p95 6.1ms p99 6.3ms max 20.7ms
processing   drained 20000/20000 in 2.7s = 7545/s   e2e p50 8507ms p95 13611ms p99 14075ms max 14189ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 2000 (busiest worker 2000)   handler threads 2000   JVM peak platform threads 2022
database     60009 row writes (ins 20002, upd 20005, del 20002) = 3.00/event   WAL 31.1MB = 1.6KB/event
statements   20677 calls = 1.03/event   claim 408 calls x 49.0 rows   finalize batched 245 calls x 81.6 rows, single 14   release 2   retry 0   other 8

c500-200ms-virtual
publish      20000 in 13.6s = 1466/s   p50 5.9ms p95 6.1ms p99 6.3ms max 25.5ms
processing   drained 20000/20000 in 2.6s = 7603/s   e2e p50 8502ms p95 13485ms p99 13968ms max 14113ms   handlings=20000 retries=0
concurrency  peak in-flight handlers 2000 (busiest worker 2000)   handler threads 20000   JVM peak platform threads 45
database     60009 row writes (ins 20002, upd 20005, del 20002) = 3.00/event   WAL 31.5MB = 1.6KB/event
statements   20644 calls = 1.03/event   claim 404 calls x 49.5 rows   finalize batched 211 calls x 94.7 rows, single 19   release 2   retry 0   other 8

hotkey-platform
publish      5000 in 7.5s = 663/s   p50 6.0ms p95 6.2ms p99 7.0ms max 17.7ms
processing   drained 5000/5000 in 22.9s = 218/s   e2e p50 5110ms p95 15153ms p99 17571ms max 20614ms   handlings=5000 retries=0
concurrency  peak in-flight handlers 6 (busiest worker 3)   handler threads 9   JVM peak platform threads 49
database     32440 row writes (ins 10150, upd 12140, del 10150) = 6.49/event   WAL 12.3MB = 2.5KB/event
statements   33586 calls = 6.72/event   claim 6593 calls x 1.3 rows   finalize batched 151 calls x 2.0 rows, single 4694   release 3564   retry 0   other 13584

hotkey-virtual
publish      5000 in 6.5s = 770/s   p50 5.0ms p95 6.2ms p99 7.9ms max 18.0ms
processing   drained 5000/5000 in 48.6s = 103/s   e2e p50 16533ms p95 36176ms p99 40685ms max 48268ms   handlings=5000 retries=0
concurrency  peak in-flight handlers 8 (busiest worker 8)   handler threads 5000   JVM peak platform threads 63
database     106154 row writes (ins 11668, upd 82818, del 11668) = 21.23/event   WAL 43.9MB = 9.0KB/event
statements   100930 calls = 20.19/event   claim 4596 calls x 9.5 rows   finalize batched 848 calls x 2.8 rows, single 2656   release 38893   retry 0   other 48937

```

All eleven: `invariants lost=0 duplicates=0 unexpected=0 lockOverlaps=0 storage: events=0 locks=0`, `RESULT PASS`.

## Next

1. ADR-0035 with the virtual executor on `hot-key`: the bounded wait
   should turn most of the 7.8 busy hits per event into a park.
2. Memory. Peak platform threads is the only footprint figure here;
   RSS and heap under 2 000 blocking handlers would complete the
   picture for finding 1.
3. A blocking handler that really blocks — a JDBC statement or an
   HTTP call instead of `Thread.sleep` — with JFR's
   `jdk.VirtualThreadPinned` event on, to put a number on "nothing
   pins" rather than infer it from equal throughput.
