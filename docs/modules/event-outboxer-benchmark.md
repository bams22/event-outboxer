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
database     1035 row writes (ins 414, upd 217, del 404) = 5.18/event
invariants   lost=0 duplicates=0 unexpected=0 lockOverlaps=0 (graded)   storage: events=0 locks=0
RESULT       PASS
```

The invariant half also runs as a test: `BenchmarkSmokeIT` executes
the `smoke` preset under `./mvnw -pl event-outboxer-benchmark -P it verify`
and asserts the verdict, never a number.

## Presets

| `--bench.scenario=` | What it shows | Defaults |
|---|---|---|
| `smoke` | the plumbing works; also the IT | 200 events, 2 workers, 16 lock keys, lease locker |
| `throughput` | the engine's own ceiling | 20 000 events, 4 types, 3 workers, pool 4, batch 50, no lock key |
| `hot-key` | the price of entity locking | 5 000 events on 8 lock keys, 5 ms of work, lease locker — rerun with `--bench.lock=noop` for the baseline where overlaps are *expected* |
| `failures` | the retry path and its DB cost | 5 000 events, 10 % of first attempts return `retry`, backoff shortened to 200 ms |
| `backlog` | drain rate after an outage | 20 000 events published first, fleet started afterwards |

Every knob is overridable: `events`, `event-types`, `lock-keys`,
`workers`, `publisher-threads`, `handler-pool-size`, `claim-batch-size`,
`poll-min-interval`, `poll-max-interval`, `executor` (`platform` |
`virtual`), `lock` (`noop` | `postgres-lease` | `postgres-advisory`),
`finalize-batching`, `handler-work-time`, `failure-rate`,
`workers-after-publish`, `drain-timeout`, `payload-bytes`,
`connection-pool-size`, plus `worker-prop.<any starter property>` for
everything else. An unknown key fails fast and lists the known ones.

Harness defaults are **not** production defaults where it matters for
the measurement: `poll-min-interval` is 100 ms (starter: 500 ms) and
`poll-max-interval` 1 s (starter: 10 s), because steady-state latency
is one of the things being measured. The report embeds the effective
scenario, so a number never travels without its configuration.

## What is graded and what is only reported

| Invariant (fails the run) | Rule |
|---|---|
| no lost event | every published sequence number has ≥ 1 successful handling |
| no duplicate | successful handlings per event ≤ 1 (the in-process fleet has no crash injection yet, so any duplicate is a bug) |
| nothing unexpected | no handling for a sequence number that was never published |
| lock-key exclusivity | two handlings of the same key never overlap — graded only when a real locker is configured; with `lock=noop` overlaps are counted and shown as information |
| storage is clean | after the graceful stop, `events` has no rows and `entity_locks` is empty |

Numbers — publish and end-to-end latency percentiles, handled/s,
retries, row writes per event — are information. They never fail a
run and are never a CI gate.

The database cost figure comes from `pg_stat_user_tables` over the
`event_outboxer` schema, sampled before publishing and after the fleet
has stopped (a PostgreSQL backend flushes its counters on exit; idle
backends may hold them back for up to ten seconds, which is why the
closing sample waits for the connections to close). Heartbeats and
worker registration during the run are included: they are real cost.

## How it is built

```
BenchmarkRunner  main: options → target → BenchmarkRun → ReportWriter
BenchmarkRun     database up → target.open → [workers] → publish → drain → close → sample → grade
target/          BenchmarkTarget · TargetSession · BenchmarkPublisher · BenchmarkEvent   (the SUT seam)
target/outboxer  OutboxerTarget: 1 publish-only Spring context + N worker contexts, BenchEventHandler → Ledger
ledger/          Handling · Ledger · InMemoryLedger
verify/          InvariantChecker → InvariantReport
db/              DatabaseHandle (testcontainers | external) · PgProbe · TableWrites · StorageState
report/          BenchmarkReport · LatencyStats · ReportWriter
scenario/        Scenario (+ presets) · ExecutorType · LockType
```

The **system under test is an interface**. `BenchmarkRun` only knows
`BenchmarkTarget`; the event-outboxer implementation boots one
publish-only context (ADR-0029) for the driver and one Spring context
per worker with an explicit `event-outboxer.worker.id`, its own
connection pool, executors and pollers. Contexts share nothing but the
database — not even the same-JVM poller wake hub — so workers discover
events by polling exactly as separate pods do. A team can measure
another outbox with the same scenarios by implementing the three
target types in its own repository; that comparison is deliberately
out of scope here.

## Limits of the in-process fleet

- Publisher and workers share one JVM's CPU and garbage collector,
  which understates what separate pods achieve.
- A crash cannot be faked honestly: an abandoned Spring context keeps
  finalizing on its handler threads. The duplicate-under-crash claim
  therefore has no scenario yet.
- Overlap detection compares timestamps from one clock; a fleet across
  hosts would need a skew tolerance.

The forked fleet (ADR-0034 phase 2 — one JVM per worker launched from
the exec jar, `SIGKILL` mid-batch, PostgreSQL restart under the fleet,
a `bench.handled` ledger table instead of the in-memory one) addresses
all three and is the prerequisite for any headline "lost = 0 under
crashes" claim.

## Reporting policy

Numbers published in this repository's docs must come from a run
against an external PostgreSQL, state the hardware, and include the
`hot-key` scenario with the locker on. A performance change to the
engine ships with a before/after report from this harness in its PR.
