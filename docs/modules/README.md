# Module documentation

One page per Maven module: why it exists, what it does, when to use
it, and how to configure it (Spring Boot and plain Java). For the
one-line pick-list and coordinates see
[ARTIFACTS.md](../../ARTIFACTS.md); for architecture and data flows
see [ARCHITECTURE.md](../ARCHITECTURE.md).

## Foundation

| Module | One-liner |
|---|---|
| [event-outboxer-bom](event-outboxer-bom.md) | Versions-only BOM — import it, drop `<version>` everywhere else. |
| [event-outboxer-api](event-outboxer-api.md) | What applications program against: publisher, handler, listener, failure chain, domain, exceptions. |
| [event-outboxer-spi](event-outboxer-spi.md) | Ports for adapters (`EventStore`, `EntityLocker`, `EventSerializer`, …) + the abstract contract tests. |
| [event-outboxer-core](event-outboxer-core.md) | The Spring-free engine: pollers, dispatcher, failure handling, maintenance, `OutboxEngineBuilder`. |

## Storage

| Module | One-liner |
|---|---|
| [event-outboxer-storage-postgres](event-outboxer-storage-postgres.md) | Production storage: PG 15+, `SKIP LOCKED` claims, shipped Flyway/Liquibase migrations. |
| [event-outboxer-storage-inmemory](event-outboxer-storage-inmemory.md) | Test infrastructure only (ADR-0020) — reference implementation for the contract tests. |

## Serialization

| Module | One-liner |
|---|---|
| [event-outboxer-serializer-jackson](event-outboxer-serializer-jackson.md) | Default JSON serializer (`jackson-json`, JSONB text lane, evolution-friendly). |
| [event-outboxer-serializer-protobuf](event-outboxer-serializer-protobuf.md) | Schema-first Protobuf (`protobuf`, BYTEA bytes lane); coexists with Jackson per event type. |

## Entity locking and cache

| Module | One-liner |
|---|---|
| [event-outboxer-lock-postgres-lease](event-outboxer-lock-postgres-lease.md) | Recommended PG locker: lease table, TTL honoured, pgBouncer-safe, no pinned connections. |
| [event-outboxer-lock-postgres-advisory](event-outboxer-lock-postgres-advisory.md) | `pg_advisory_lock` opt-out: immediate clean-crash release, at the cost of pinned connections. |
| [event-outboxer-lock-redis](event-outboxer-lock-redis.md) | Redis/KeyDB locker: `SET NX PX` + fencing-token release. |
| [event-outboxer-cache-redis](event-outboxer-cache-redis.md) | Shared metrics-snapshot cache — one consistent backlog view per fleet. |

## Observability

| Module | One-liner |
|---|---|
| [event-outboxer-metrics-micrometer](event-outboxer-metrics-micrometer.md) | Micrometer listener: `event_outboxer.*` counters/timers per event type. |
| [event-outboxer-tracing-otel](event-outboxer-tracing-otel.md) | OpenTelemetry tracer: publish→handle trace continuity (OTel agent / SDK). |
| [event-outboxer-tracing-micrometer](event-outboxer-tracing-micrometer.md) | Micrometer Tracing tracer — the Boot Actuator-native alternative. |

## Operations

| Module | One-liner |
|---|---|
| [event-outboxer-admin-actuator](event-outboxer-admin-actuator.md) | `outboxadmin` Actuator endpoint: inspect / re-enable / purge on the management port. |
| [event-outboxer-admin-rest](event-outboxer-admin-rest.md) | Opt-in REST admin surface on the app port, guarded by a configurable authority. |

## Testing and integration

| Module | One-liner |
|---|---|
| [event-outboxer-testkit](event-outboxer-testkit.md) | Deterministic handler tests: `ManualEngine.tick()`, `SettableClock`, recording listener, assertions. |
| [event-outboxer-spring-boot-starter](event-outboxer-spring-boot-starter.md) | Spring Boot 3.5+ autoconfiguration, transaction integration, lifecycle, health. |

## Which modules do I need?

- **Spring Boot + PostgreSQL (typical production)**: `spring-boot-starter` + `storage-postgres` (+ `lock-postgres-lease` if handlers use `extractLockKey`, + `metrics-micrometer`, + a tracing adapter).
- **Plain Java**: `core` + `storage-postgres` + `serializer-jackson` (or `-protobuf`) + a locker if needed.
- **Tests**: `testkit` (test scope); `storage-inmemory` + `@Import(OutboxInMemoryTestConfiguration.class)` for DB-less Spring tests.
- **Custom adapter**: `spi` + its `tests`-classifier contract tests.

Always import the [BOM](event-outboxer-bom.md) first.
