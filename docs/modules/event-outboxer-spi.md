# event-outboxer-spi

The adapter contract: Service Provider Interfaces that decouple the
engine from any concrete storage, lock backend, serialization format,
clock or tracing system. Also ships the reusable **abstract contract
tests** (as a `tests`-classifier jar) that every adapter must pass.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-spi` (plus `…:event-outboxer-spi:<version>:tests` for the contract tests) |
| Java package | `io.github.bams22.outboxer.spi` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), `slf4j-api`, `jspecify` |
| Who adds it | Transitive for applications; **direct** for authors of custom adapters |

## Why it exists

[ADR-0010](../adr/0010-storage-agnostic-core-via-spi.md): everything
the engine needs from the outside world is a port defined here. The
core depends on ports only; adapters (`-storage-postgres`, `-lock-*`,
`-serializer-*`, `-cache-redis`, `-tracing-*`) implement them without
ever depending on the core. That keeps semantics in one place and
makes backends swappable and independently testable.

## The ports

| Port | Implemented by | Purpose |
|---|---|---|
| `EventStore` | [postgres](event-outboxer-storage-postgres.md), [inmemory](event-outboxer-storage-inmemory.md) | save (+ dedup coalescing), claim, finalize, release, reclaim, sweep, metrics snapshot |
| `WorkerRegistry` | postgres, inmemory | register / heartbeat / findDead / removeDead — the crashed-worker detection substrate ([ADR-0005](../adr/0005-workers-heartbeat-table.md)) |
| `EntityLocker` (+ `LockHandle`) | [postgres-lease](event-outboxer-lock-postgres-lease.md), [postgres-advisory](event-outboxer-lock-postgres-advisory.md), [redis](event-outboxer-lock-redis.md), inmemory, `EntityLocker.NOOP` | distributed business-key lock for `extractLockKey` ([ADR-0012](../adr/0012-extract-lock-key-on-handler.md)) |
| `EventSerializer` | [jackson](event-outboxer-serializer-jackson.md), [protobuf](event-outboxer-serializer-protobuf.md), custom | payload ↔ `SerializedPayload`; `format()` id persisted per event ([ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md)) |
| `EventSerializerRegistry` | final class (not implementable) | routes deserialization by the stored `payload_format`; validates format ids at startup |
| `Clock` | `Clock.system()`, testkit `SettableClock` | time source — adapters must never call `Instant.now()` directly |
| `ConnectionSupplier` | starter (`DataSourceUtils`-backed), custom | JDBC connection lease that makes `publish()` join the caller's transaction ([ADR-0002](../adr/0002-participate-in-client-transaction.md)) |
| `OutboxAdmin` (+ `AdminCursor`, `ArchiveCursor`) | postgres, inmemory | operational surface: list / re-enable / replay / purge, keyset-paginated ([ADR-0019](../adr/0019-admin-and-retention-surface.md), [ADR-0033](../adr/0033-archive-dedup-key-and-replay-from-archive.md)) — deliberately separate from `EventStore` so hot-path adapters need not implement it; the two replay methods are `default` (validate, then the archive-less answer), so only an adapter with an archive overrides them |
| `MetricsSnapshotCache` | built-in `noop()` / `inMemory(clock, ttl)`, [cache-redis](event-outboxer-cache-redis.md) | caches `metricsSnapshot()` for health/gauges; fail-safe (`get()` returns empty on any backend error) |
| `OutboxTracer` (+ `PublishSpan`, `ProcessSpan`) | [otel](event-outboxer-tracing-otel.md), [micrometer](event-outboxer-tracing-micrometer.md), `OutboxTracer.NOOP` | PRODUCER/CONSUMER spans and the flat string-map trace carrier ([ADR-0023](../adr/0023-tracing-spi-port-and-adapters.md)) |
| `OutboxMetricsSnapshot` | record | per-status totals + per-type stats feeding health and gauges |
| `OutboxTraceAttributes` | constants | shared span attribute keys so all tracer adapters emit identical metadata |

## Contracts every adapter must honour

These are stated in the port javadoc and verified by the contract tests:

1. **Optimistic concurrency, not exceptions** ([ADR-0014](../adr/0014-optimistic-locking-via-version-field.md)).
   `claim()` returns each event with its *new* `version`; every
   finalize (`markProcessed`, `markForRetry`, `markDisabled`,
   `forceReclaim`, `release`) is guarded by
   `id + version + claimed_by` and returns `false` — never throws —
   when the row moved on. `StorageException` is reserved for technical
   failures.
2. **Attempts accounting.** `release` / `releaseClaimed` return an
   event to `PENDING` *without* bumping `attempts` (contention,
   backpressure, shutdown); `forceReclaim` / `sweepStale` /
   `reclaimOrphans` bump it (crash-path semantics). This is what keeps
   backpressure from pushing events toward `DISABLED`.
3. **Transaction participation.** `save`/`saveAll` run on the
   connection from `ConnectionSupplier.get()`; in a transactional
   context that connection is the caller's and must not be
   committed/rolled back by the adapter. `release()` is idempotent.
4. **Dedup coalescing** ([ADR-0021](../adr/0021-dedup-key-single-inflight-per-key.md)):
   `save` with a dedup key is a conditional insert; `saveAll` rejects
   dedup keys; `lockPendingByDedupKey` pins the coalesced-into row for
   the caller's transaction.
5. **Locking:** "busy" is `Optional.empty()`, never an exception;
   `LockHandle.close()` is idempotent and declared without `throws`.
6. **Serializer format ids** are lowercase kebab-case, ≤ 64 chars, and
   never renamed once events written with them may exist.
7. **Exception envelope:** adapters wrap native errors
   (`SQLException`, Lettuce exceptions) into the
   `StorageException` / `LockException` hierarchy from
   [`event-outboxer-api`](event-outboxer-api.md).
8. **Thread safety** is required of every port implementation.

## The contract test kit

Published as `io.github.bams22:event-outboxer-spi:<version>:tests`,
package `io.github.bams22.outboxer.spi.contracts` — abstract JUnit 5
specifications that adapter test classes extend:

| Base class | You implement | Covers |
|---|---|---|
| `AbstractEventStoreContractTest` (~44 tests) | `newStore()`, `backdateClaim(id, at)` | full lifecycle, every OCC race, orphan recovery, force-reclaim, no-duplicate-claims under concurrency |
| `AbstractWorkerRegistryContractTest` | `newRegistry()` (+ `backdateHeartbeat` if your adapter uses the DB clock) | register/heartbeat/findDead/removeDead |
| `AbstractEntityLockerContractTest` | `newLocker()` (+ opt-in `supportsTtlExpiry()` / `forceExpire(key)`) | non-blocking tryLock, busy semantics, release, TTL expiry |
| `AbstractOutboxAdminContractTest` | `newStore()`, `newAdmin()` | filtering, ordering, keyset paging, re-enable, purge |

## How to build a custom adapter

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-spi</artifactId>
</dependency>
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-spi</artifactId>
    <type>test-jar</type>
    <scope>test</scope>
</dependency>
```

1. Implement the port(s) — `EventStore` + `WorkerRegistry` for a new
   storage backend, `EntityLocker` for a new lock backend,
   `EventSerializer` for a new payload format.
2. Extend the matching contract test(s) in your test sources and make
   them pass — they are the behavioural specification.
3. Do **not** depend on `event-outboxer-core` (the shipped adapters
   enforce this with a `bannedDependencies` rule; follow suit).
4. Wire the adapter into `OutboxEngineBuilder` (plain Java) or expose
   it as a Spring bean — the starter's `@ConditionalOnMissingBean`
   wiring picks user beans over its own for every port.

## Related

- [ARCHITECTURE.md §Key components](../ARCHITECTURE.md#key-components) — port-to-implementation matrix.
- [STORAGE.md](../STORAGE.md) — reference semantics of the PostgreSQL implementation.
- ADRs: [0002](../adr/0002-participate-in-client-transaction.md), [0005](../adr/0005-workers-heartbeat-table.md), [0010](../adr/0010-storage-agnostic-core-via-spi.md), [0014](../adr/0014-optimistic-locking-via-version-field.md), [0019](../adr/0019-admin-and-retention-surface.md), [0023](../adr/0023-tracing-spi-port-and-adapters.md), [0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md).
