# ADR-0034: Benchmark and invariant harness module

## Status

Accepted

## Date

2026-09-04

## Context

The library already carries two performance-motivated designs whose
effect is stated only analytically:

- **Capacity-coupled polling** (ADR-0004 amendment) claims to remove
  the `claim-batch-size / poll-min-interval` throughput ceiling
  ("20 events/s per type with defaults") and "~2 wasted hot-table
  writes per event" under overload.
- **Group-commit finalize batching** (ADR-0014 amendment) claims "up
  to ~batch-size× fewer finalize round-trips on hot types".

Neither number was measured in this repository. There is no way to
confirm them, to notice a regression after the next poller refactor,
or to show a prospective adopter what the library does on their
hardware.

The second gap is correctness under real concurrency. Every
integration test today runs **one** engine against one PostgreSQL;
crash and recovery tests run in-process against the in-memory
adapter. Orphan reclaim, the lease locker, dedup coalescing and the
optimistic `version` guard are exactly the paths that only break in a
race between JVMs, and the at-least-once contract (ADR-0015) has never
been exercised by such a race.

Both gaps matter for adoption. A team evaluating an outbox library
asks, in this order: *will it lose or duplicate events, what happens on
a rolling deploy or a crashed pod, what does it cost the shared
database, and how does it compare to what we run today.* Raw
throughput rarely decides; failure behaviour and database cost do. A
self-authored benchmark of one's own library is discounted by default
unless it is reproducible in one command, publishes its hardware and
configuration next to every number, and includes at least one scenario
where the library looks bad.

## Alternatives considered

- **JMH micro-benchmarks on `-core`.** Fast and precise, but the hot
  path is a PostgreSQL round-trip; measuring the engine against the
  in-memory adapter says nothing about deployed behaviour. Rejected as
  the primary tool. It remains the right tool for the one pure-CPU
  component, the serializers, and may be added later.
- **A standalone project outside the reactor, like
  `examples/spring-boot-postgres`.** Keeps the default build lean, but
  the examples consume a *released* version from Central. A benchmark
  must measure the working tree, otherwise it cannot gate a
  performance change. Rejected.
- **Benchmarks as failsafe ITs under `-P it`.** Puts numbers into CI,
  where shared runners make them noise, and a wall-clock assertion
  becomes a flaky test. Rejected for numbers; accepted for the
  invariant half, which is pass/fail and belongs in `-P it`.
- **Driving a demo application with an external load tool (k6,
  Gatling).** Measures the demo's HTTP layer, its connection pool and
  its JSON parsing in front of the outbox. Rejected: the harness must
  publish through `OutboxEventPublisher` inside a transaction and
  nothing else.
- **A Jepsen-style external checker.** The strongest form of the
  invariant half, and out of proportion for an embedded library: a
  handling ledger plus a checker over it catches the same classes of
  bug (lost, duplicated, overlapping handlings) with a fraction of the
  machinery.

## Decision

### 1. A reactor module that is never published

`event-outboxer-benchmark` (package
`io.github.bams22.outboxer.benchmark`) joins the Maven reactor so it
compiles and unit-tests against the working tree on every build. It
is **not a consumer artifact**: `maven.deploy.skip`,
`skipPublishing`, `gpg.skip`, `maven.javadoc.skip`,
`maven.source.skip` and `japicmp.skip` are all `true` in its pom, it
is absent from the BOM and from `ARTIFACTS.md`. The `bench` Maven
profile repackages it into an executable Spring Boot jar
(`*-exec.jar`) so a run is one command:

```bash
./mvnw -B -ntp -DskipTests install
./mvnw -B -ntp -pl event-outboxer-benchmark -P bench -DskipTests package
java -jar event-outboxer-benchmark/target/event-outboxer-benchmark-*-exec.jar \
     --bench.scenario=throughput --bench.events=50000 --bench.workers=3
```

Without `--bench.jdbc-url` the runner starts a disposable PostgreSQL
through Testcontainers; with it, the run targets an existing database
(the honest configuration for published numbers).

### 2. The system under test is an interface

The driver knows the target only through three types:

| Type | Responsibility |
|---|---|
| `BenchmarkTarget` | `name()`, `open(BenchmarkEnvironment)` → a session |
| `TargetSession` | `publisher()`, `startWorkers()`, `close()` |
| `BenchmarkPublisher` | `publish(BenchmarkEvent)` durably, inside the target's own transaction semantics |

The harness supplies the database coordinates, the scenario and a
`Ledger` the target's handler must write every handling into. Only the
event-outboxer target ships in this repository. The interface exists
so that a team evaluating the library can write a second target for
whatever outbox it runs today, in its own repository, and run the same
scenarios on the same infrastructure. That comparison is deliberately
**out of scope here** and never appears in these docs.

### 3. The event-outboxer target runs the library as deployed

The target boots the **Spring Boot starter**, not `OutboxEngineBuilder`
directly: `event-outboxer-storage-postgres`, the lease locker
(ADR-0022, switchable to advisory or noop per scenario), the
starter-managed Flyway instance (ADR-0028) and the starter's
`TransactionAwareDataSourceProxy` wiring (ADR-0002). What is measured
is what a team deploys.

- **Publisher side**: one `publish-only` context (ADR-0029) whose
  `OutboxEventPublisher` is called inside a `TransactionTemplate`, one
  business transaction per event by default.
- **Worker fleet, phase 1 (this ADR)**: `N` additional Spring contexts
  in the driver JVM, each with an explicit `event-outboxer.worker.id`
  (`bench-w<i>`), its own connection pool, executors and pollers.
  Contexts do not share a `PollerWakeHub`, so workers rely on polling
  exactly like separate pods do.
- **Worker fleet, phase 2 (follow-up, not in this ADR's code)**: one
  JVM per worker forked from the exec jar, so a worker can be
  `SIGKILL`ed mid-batch and PostgreSQL can be restarted underneath the
  fleet. The in-process fleet cannot fake a crash honestly: an
  abandoned context keeps finalizing on its handler threads.

### 4. Every handling goes into a ledger

The target's handler records `(seq, eventType, attempt, workerId,
thread, startedAt, finishedAt, outcome)` for every invocation,
including retries. The in-process fleet uses an in-memory ledger; the
forked fleet will use the `bench.handled` table in the same
PostgreSQL. The ledger is the only source of truth for both metrics
and invariants: the harness never trusts the library's own listener
callbacks or metrics to grade the library.

### 5. Invariants are pass/fail; numbers are information

After the drain the checker grades the ledger against the published
set:

| Invariant | Rule | Expected |
|---|---|---|
| **No lost event** | every published `seq` has ≥ 1 successful handling | always 0 lost |
| **No unexplained duplicate** | successful handlings per `seq` > 1 | 0 without chaos; every duplicate attributable to an injected crash with chaos |
| **Lock-key exclusivity** | two handlings with the same lock key never overlap in `[startedAt, finishedAt]` | 0 overlaps when a locker is configured; overlaps *expected* with `lock.type=noop` (that is the hot-key scenario's baseline) |
| **Storage is clean** | after the drain the `events` table holds no rows for the run and `entity_locks` is empty | always |

A run whose invariants fail exits non-zero and writes the report
anyway. Throughput and latency never fail a run.

### 6. Metrics come from the ledger and from PostgreSQL statistics

- Publish: calls/s and per-call latency percentiles.
- End-to-end: `firstSuccessfulHandling.finishedAt − publishedAt` per
  event, p50/p95/p99/max, plus handled/s and drain time.
- Database cost: `n_tup_ins + n_tup_upd + n_tup_del` over the
  `event_outboxer` schema from `pg_stat_user_tables`, sampled before
  and after, divided by events. The ledger table is outside that
  schema and does not count.
- Retries: handlings with `attempt > 1`.

### 7. Scenarios are presets with overrides, and the effective config is always printed

| Preset | What it shows |
|---|---|
| `smoke` | 200 events, 2 workers: does the plumbing work (also the `-P it` test) |
| `throughput` | many events, no lock key, no simulated work: the engine's own ceiling |
| `hot-key` | all events on a handful of lock keys: the cost of entity locking, run with and without a locker |
| `failures` | a share of first attempts return `retry`: the retry path and its DB cost |
| `backlog` | publish everything first, then start the fleet: drain rate after an outage |

Every knob (`workers`, `eventTypes`, `handlerPoolSize`,
`claimBatchSize`, `pollMinInterval`, `executorType`, `lockType`,
`finalizeBatching`, `handlerWorkTime`, `failureRate`, ...) is
overridable from the command line, and the JSON report embeds the
effective scenario, the JVM and host description and the PostgreSQL
version. A number without its configuration is not a result.

### 8. Reporting policy

Runs are manual or nightly, never a CI gate. Numbers published in this
repository's docs must come from a run against an external PostgreSQL
(not Testcontainers on the build host), state the hardware, and
include the `hot-key` scenario with the locker on. A performance
change to the engine ships with a before/after report from this
harness in its PR description.

## Rationale

*As deployed, not as convenient.* Booting the starter costs a few
seconds per context and ties the harness to starter property names,
but it is the only setup where the numbers are the numbers a team will
see. Publish-only mode already exists for exactly this split.

*In the reactor, unpublished.* The build-time cost is compiling one
module and a handful of unit tests; the benefit is that the harness
cannot silently drift from the SPI, and a performance PR can run it on
the same commit it changes.

*Ledger over library callbacks.* Grading the library with its own
listener would let a bug in the listener hide a bug in the dispatcher.
The handler's own record is independent evidence.

*Invariants gate, numbers inform.* Wall-clock numbers vary with the
host; "lost = 0" does not. Keeping the two separate is what lets the
invariant half run under `-P it` while the numbers stay out of CI.

*An interface for the target.* The comparison that convinces a team is
against their current solution. Designing for that up front costs
three small types and keeps the comparison out of this repository.

## Consequences

**Users of the library.** Nothing on their classpath changes. The
docs gain a page with reproducible numbers and their configuration,
and the harness doubles as a capacity-planning tool: run the
`throughput` preset against a staging database with the pool sizes
you intend to deploy.

**Maintainers.** A new module to keep in sync with starter properties
and with the SPI. Performance changes now need evidence: the harness
report becomes part of the PR. The phase-2 forked fleet and crash
chaos are the next step and should follow before any headline
"lost = 0 under crashes" claim is made in the README.

**Operations.** None directly. The scenarios mirror the questions an
on-call engineer asks (drain after outage, retry storms, hot keys), so
the reports are also the documentation for those situations.

**Negative.** Benchmark code attracts tuning of the benchmark instead
of the library; the reporting policy (external PostgreSQL, hot-key
included) is the guard. The in-process fleet shares one JVM's garbage
collector and CPU between publisher and workers, which understates
what separate pods achieve; phase 2 removes that bias.

## Related decisions

- [ADR-0002](0002-participate-in-client-transaction.md) — the
  publisher side publishes inside a real transaction.
- [ADR-0004](0004-per-event-type-worker-isolation.md) — capacity-coupled
  polling, the first claim the harness must confirm.
- [ADR-0014](0014-optimistic-locking-via-version-field.md) — group-commit
  finalize batching, the second claim.
- [ADR-0015](0015-at-least-once-semantics.md) — the contract the
  invariants grade.
- [ADR-0016](0016-maven-module-structure.md) — amended: 21st module,
  never published.
- [ADR-0020](0020-no-inmemory-storage-in-production.md) — the harness
  runs on PostgreSQL only; the in-memory adapter measures nothing.
- [ADR-0022](0022-lease-table-postgres-entity-locker.md) — the locker
  the `hot-key` scenario prices.
- [ADR-0029](0029-publish-only-is-explicit.md) — the publisher context
  runs in publish-only mode.
