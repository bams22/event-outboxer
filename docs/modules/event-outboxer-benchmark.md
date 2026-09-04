# event-outboxer-benchmark

Load and invariant harness (ADR-0034). Drives the library **as
deployed** — Spring Boot starter, PostgreSQL storage, lease locker —
through configurable scenarios, grades the at-least-once invariants
from its own handling ledger, and writes a self-describing JSON report.

| | |
|---|---|
| Coordinates | none — **never published**; part of the reactor so it always builds against the working tree |
| Java package | `io.github.bams22.outboxer.benchmark` |
| Runs on | PostgreSQL only (disposable Testcontainers instance by default, or an external one) |
| Spring | yes — boots the starter to measure what a team actually deploys |

## Why it exists

The CHANGELOG states performance effects ("removes the
`claim-batch-size / poll-min-interval` ceiling", "up to ~batch-size×
fewer finalize round-trips") that were derived, not measured. And no
integration test runs more than one engine against one database, so
the at-least-once contract (ADR-0015) had never been exercised by a
real race between workers. This module closes both gaps and doubles as
the evidence a team asks for before adopting the library: not raw
throughput, but *lost = 0*, database cost per event, and behaviour
under retries and hot keys.

## Run it

```bash
# once: build the library into the local repository
./mvnw -B -ntp -DskipTests install

# build the executable jar
./mvnw -B -ntp -pl event-outboxer-benchmark -P bench -DskipTests package

# run a preset on a disposable PostgreSQL 15 (needs Docker)
java -jar event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar \
     --bench.scenario=throughput

# chaos: forked fleet, two workers SIGKILLed and replaced, then a PostgreSQL crash
java -jar event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar \
     --bench.scenario=crash --bench.pg-restart=crash --bench.pg-restart-at=0.6

# the same against your own database — the only setup whose numbers count
java -jar event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar \
     --bench.scenario=throughput --bench.workers=3 \
     --bench.jdbc-url=jdbc:postgresql://db:5432/bench \
     --bench.jdbc-user=bench --bench.jdbc-password=secret
```

Exit code `0` = passed, `1` = an invariant failed or the drain timed
out, `2` = the run could not be executed. The JSON report lands in
`target/bench/<scenario>-<utc-stamp>.json` (override with
`--bench.report-dir`); a summary is printed to stdout:

```
event-outboxer  scenario=smoke  events=200 workers=2 types=1 lockKeys=16 pool=3 batch=10 poll=100ms lock=postgres-lease exec=platform finalizeBatching=true work=0ms failureRate=0.00
environment  java 25.0.2  Linux 7.0.0 (amd64)  cpus=20 heap=7904MB  postgres 15.17 (testcontainers:postgres:15)  library working-tree
publish      200 in 55ms = 3579/s   p50 0.8ms p95 1.3ms p99 11.6ms max 12.1ms
processing   drained 200/200 in 1.4s = 141/s   e2e p50 181ms p95 198ms p99 1380ms max 1387ms   handlings=200 retries=0
concurrency  peak in-flight handlers 6 (busiest worker 3)   handler threads 6   JVM peak platform threads 61
database     1035 row writes (ins 414, upd 217, del 404) = 5.18/event
invariants   lost=0 duplicates=0 unexpected=0 lockOverlaps=0 (graded)   storage: events=0 locks=0
RESULT       PASS
```

The `concurrency` line is measured, not read off the configuration:
the peak is the largest number of ledger entries whose
`startedAt`–`finishedAt` intervals overlap, fleet-wide and within the
busiest worker; `handler threads` counts distinct thread names (one
per handling with the virtual-thread executor); the JVM figure is
`ThreadMXBean`'s peak platform thread count of the process that ran
the fleet (driver threads included, `n/a` for a forked fleet) —
virtual threads are not in it, which is the point of printing it.

The invariant half also runs as tests under
`./mvnw -pl event-outboxer-benchmark -P it verify`: `BenchmarkSmokeIT`
(in-process), `BenchmarkCrashIT` (forked fleet, two workers `SIGKILL`ed
and replaced) and `BenchmarkPostgresRestartIT` (forked fleet, database
fast-restarted). Each asserts the verdict, never a number.

## Presets

| `--bench.scenario=` | What it shows | Defaults |
|---|---|---|
| `smoke` | the plumbing works; also the IT | 200 events, 2 workers, 16 lock keys, lease locker |
| `throughput` | the engine's own ceiling | 20 000 events, 4 types, 3 workers, pool 4, batch 50, no lock key |
| `hot-key` | the price of entity locking | 5 000 events on 8 lock keys, 5 ms of work, lease locker — rerun with `--bench.lock=noop` for the baseline where overlaps are *expected* |
| `failures` | the retry path and its DB cost | 5 000 events, 10 % of first attempts return `retry`, backoff shortened to 200 ms |
| `backlog` | drain rate after an outage | 20 000 events published first, fleet started afterwards |
| `crash` | orphan reclaim, lease takeover, duplicate accounting | forked fleet of 3, 5 000 events on 32 keys, two workers `SIGKILL`ed at 30 % and respawned, fast recovery timers |
| `pg-restart` | pool recovery, finalize failures, stale-claim sweep | forked fleet of 3, 5 000 events, PostgreSQL fast-restarted at 40 %; `--bench.pg-restart=crash` for a crash with WAL replay |

Every knob is overridable: `events`, `event-types`, `lock-keys`,
`workers`, `publisher-threads`, `handler-pool-size`, `claim-batch-size`,
`poll-min-interval`, `poll-max-interval`, `executor` (`platform` |
`virtual`), `lock` (`noop` | `postgres-lease` | `postgres-advisory` |
`redis` — with `redis-uri`, or a disposable `redis-image` container),
`payload` (`jackson` | `protobuf`: the write format, same three fields
either way),
`finalize-batching`, `handler-work-time`, `slow-key-share` +
`slow-key-work-time` (route a share of events to one dedicated slow lock
key `key-slow` with its own work time: the mixed workload of ADR-0035),
`failure-rate`,
`workers-after-publish`, `drain-timeout`, `payload-bytes`,
`connection-pool-size`, `fleet` (`in-process` | `forked`),
`worker-jvm-args`, `kill-workers`, `kill-at`, `respawn-killed`,
`pg-restart` (`none` | `fast` | `crash`), `pg-restart-at`, plus
`worker-prop.<any starter property>` for everything else. An unknown
key fails fast and lists the known ones.

Harness defaults are **not** production defaults where it matters for
the measurement: `poll-min-interval` is 100 ms (starter: 500 ms) and
`poll-max-interval` 1 s (starter: 10 s), because steady-state latency
is one of the things being measured. The chaos presets additionally
shorten the recovery timers (heartbeat 1 s, dead-threshold 5 s,
orphan-recovery 2 s, stale-claim-sweep 5 s, handler-max-runtime 10 s,
lock-ttl 15 s), because the production values (30 s, 5 min, 10 min)
would turn a recovery into a coffee break. Those timers bound the
recovery times a chaos run reports. The report embeds the effective
scenario, so a number never travels without its configuration.

## What is graded and what is only reported

| Invariant (fails the run) | Rule |
|---|---|
| no lost event | every published sequence number has ≥ 1 successful handling |
| no unexplained duplicate | successful handlings per event ≤ 1, except duplicates a chaos event explains: one of the event's successful handlings lies within ±10 s of a `SIGKILL` of its own worker or of a database restart (its finalize was lost — the at-least-once contract at work). Without chaos every duplicate is a bug |
| nothing unexpected | no handling for a sequence number that was never published |
| lock-key exclusivity | two handlings of the same key never overlap — graded only when a real locker is configured; with `lock=noop` overlaps are counted and shown as information |
| storage is clean | after the graceful stop, `events` has no rows and no live lease that somebody alive should have released is left in `entity_locks`; leases owned by killed workers and leases acquired before a database outage are discounted |

Numbers — publish and end-to-end latency percentiles, handled/s,
retries, row writes per event — are information. They never fail a
run and are never a CI gate.

A run starts with `VACUUM FULL` on the events table, so a previous
run's dead rows neither slow the claims nor inflate the size figures —
the harness assumes a dedicated database. The database cost figures
are row writes from `pg_stat_user_tables` over the `event_outboxer`
schema, WAL bytes from `pg_current_wal_lsn` (both sampled before
publishing and after the fleet has stopped) and the events table size
right after the publish phase. The closing sample waits for every
connection to close: a PostgreSQL backend flushes its counters on
exit, and idle backends may hold them back for up to ten seconds.
When the server preloads `pg_stat_statements`
(`shared_preload_libraries`), the report also carries a `statements`
block: calls and rows per statement class — claim, insert, batched
and single finalize, release, retry, disabled, other — so a change in
round trips per event is visible directly. The `UPDATE` shapes are
told apart by their SET lists, since `pg_stat_statements` normalises
literals. Heartbeats and
worker registration during the run are included: they are real cost.
A *crash* restart of PostgreSQL resets the cumulative statistics; the
report then marks the figure unreliable. A fast restart persists them.

## How it is built

```
BenchmarkRunner  main: options → target → BenchmarkRun → ReportWriter; --bench.role=worker → WorkerProcess
BenchmarkRun     database up → target.open → [workers] → publish → drain (+ chaos) → close → sample → grade
target/          BenchmarkTarget · TargetSession · BenchmarkPublisher · BenchmarkEvent   (the SUT seam)
target/outboxer  OutboxerTarget: 1 publish-only Spring context + a WorkerFleet
                 InProcessFleet (contexts in this JVM) | ForkedFleet (JVM per worker, WorkerSpec file, ready marker, log)
                 WorkerBootstrap (one place that turns a Scenario into starter properties) · WorkerProcess · BenchEventHandler → Ledger
ledger/          Handling · Ledger · InMemoryLedger (in-process) · JdbcLedger (bench.handled, forked)
verify/          InvariantChecker → InvariantReport · ChaosEvent (attribution window)
db/              DatabaseHandle (testcontainers with fixed host port + restart | external) · PgProbe · TableWrites · StorageState
report/          BenchmarkReport · LatencyStats · ReportWriter
scenario/        Scenario (+ presets) · Chaos · FleetMode · PostgresRestart · ExecutorType · LockType
```

The **system under test is an interface**. `BenchmarkRun` only knows
`BenchmarkTarget`; the event-outboxer implementation boots one
publish-only context (ADR-0029) for the driver and one worker instance
per `workers`, each with an explicit `event-outboxer.worker.id`, its
own connection pool, executors and pollers. Instances share nothing
but the database — not even the same-JVM poller wake hub — so workers
discover events by polling exactly as separate pods do. A team can
measure another outbox with the same scenarios by implementing the
three target types in its own repository; that comparison is
deliberately out of scope here.

## Two fleets

| | `--bench.fleet=in-process` (default) | `--bench.fleet=forked` |
|---|---|---|
| worker | a Spring context in the driver JVM | a JVM of its own, forked with the same code (exec jar or class path) and a JSON spec file |
| ledger | in memory, zero cost | `bench.handled` table, one small pool per JVM |
| kill | impossible — an abandoned context keeps finalizing | `SIGKILL`, optional respawn with a fresh id |
| PostgreSQL restart | supported | supported |
| cost | seconds per run | + ~2–5 s JVM boot per worker, logs and spec files under `target/bench/work/<scenario>-<stamp>/` |

The forked worker waits for `SIGTERM` (graceful stop, claims released)
or for the driver to vanish (its stdin closes), so a driver that dies
never leaves workers behind. Worker output is in `<id>.log` next to
its `<id>.json` spec and `<id>.ready` marker. Every context the target
boots registers a recovery listener that logs orphan reclaims, stale
sweeps, stuck-handler reclaims, abandoned handlers, non-busy retries
and storage errors at WARN with the worker id, and a run whose
invariants fail writes the full ledger to `handlings.csv` in the work
directory.

## Remaining limits

- Everything runs on one host: the forked fleet has separate heaps but
  shares the CPU, and overlap detection compares timestamps from one
  clock. A fleet across hosts would need a skew tolerance.
- The PostgreSQL restart needs the disposable Testcontainers database
  (the container is created with a fixed host port so its address
  survives the restart); against `--bench.jdbc-url` the option fails
  fast.
- The ±10 s attribution window can mask a genuine duplicate that
  happens to land inside it.
- A crash restart of PostgreSQL resets `pg_stat`; the database cost
  figure of such a run is marked unreliable.

## Recorded runs

Sessions are written up under [docs/benchmarks/](../benchmarks/README.md),
each with hardware, database, commit, commands and the verbatim console
summaries. First one: [2026-09-04, developer laptop](../benchmarks/2026-09-04-laptop-first-run.md)
— three row writes per event confirmed, and the hot-key path under a
locker identified as the first scene where the library looks bad.
Second: [2026-09-04, locks](../benchmarks/2026-09-04-laptop-locks.md)
— the lease locker costs PostgreSQL two extra row writes per locked
event, the Redis locker none (four Redis commands instead). When the
scenario uses the Redis locker the report carries a `redis` block:
commands per event from `INFO stats` and lock keys left behind.
Third: [2026-09-04, payload formats](../benchmarks/2026-09-04-laptop-payload-formats.md)
— Jackson and Protobuf cost the database the same for text payloads;
the run-order trap that led to `VACUUM FULL` at run start. Fourth:
[2026-09-04, group-commit convoy](../benchmarks/2026-09-04-laptop-group-commit-convoy.md)
— the harness's first engine finding: under commit-bound latency the
group-commit flush lock convoys, and every laptop number was bound by
a ~5 ms commit. Fifth:
[2026-09-04, group-commit matrix](../benchmarks/2026-09-04-laptop-group-commit-matrix.md)
— batching on vs off across 16 cells; it lost in 15. Sixth:
[2026-09-04, group commit + Redis locker](../benchmarks/2026-09-04-laptop-group-commit-redis.md)
— with a locker's round trips jittering the arrivals, batching wins on
unique keys under fsync and is irrelevant on hot keys. Seventh:
[2026-09-04, group commit after the fix](../benchmarks/2026-09-04-laptop-group-commit-after-fix.md)
— the harness's first before/after of an engine change: the
convoy-free flush path (ADR-0014 amendment), validated on the same
matrix. Eighth:
[2026-09-04, publish-only sweeper](../benchmarks/2026-09-04-laptop-publish-only-sweeper.md)
— a correctness defect (ADR-0029 amendment) found by the `crash`
preset at full size, traced with the ledger dump and the recovery
listener the harness gained for it.

## Reporting policy

Numbers published in this repository's docs must come from a run
against an external PostgreSQL, state the hardware, and include the
`hot-key` scenario with the locker on. A performance change to the
engine ships with a before/after report from this harness in its PR.
