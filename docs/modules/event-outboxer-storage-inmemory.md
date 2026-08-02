# event-outboxer-storage-inmemory

**Test infrastructure — never a production storage
([ADR-0020](../adr/0020-no-inmemory-storage-in-production.md)).**
Thread-safe in-process implementations of `EventStore`,
`WorkerRegistry`, `OutboxAdmin` and `EntityLocker`, used by the
library's own tests, the [testkit](event-outboxer-testkit.md) and
Spring test contexts.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-storage-inmemory` |
| Java package | `io.github.bams22.outboxer.storage.inmemory` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `slf4j-api` |
| Scope | **test** in consumer projects |

## Why it exists

A transactional outbox without a durable store is pointless — that is
exactly why the module is *deliberately unreachable* through
`event-outboxer.*` properties. Early versions auto-fell-back to
in-memory storage when `storage.type` was unset; an app could then
"successfully" publish events that ignored rollbacks and vanished on
restart. ADR-0020 removed the `inmemory` storage type entirely:
`event-outboxer.storage.type` has no default and an unconfigured
outbox fails startup with an actionable message.

The module is still kept as a first-class artifact because:

- core's unit tests run against it without Testcontainers;
- it is the **reference implementation** of the SPI contract tests —
  the executable definition of correct adapter behaviour that the
  PostgreSQL adapter is held to as well;
- the [testkit](event-outboxer-testkit.md)'s `OutboxTestContext`
  builds on it for plain-Java handler tests.

## What it contains

| Class | Responsibility |
|---|---|
| `InMemoryEventStore` | `EventStore` over a `ConcurrentHashMap`; per-row synchronized transitions; claim ordering (`priority DESC, runAt ASC`) and dedup-key uniqueness mirror the PostgreSQL queries. `markProcessed` removes the row — there is **no archive** |
| `InMemoryWorkerRegistry` | `WorkerRegistry`; `findDead` treats `gracefulStop` as immediately dead, like the PG adapter |
| `InMemoryOutboxAdmin` | `OutboxAdmin` over the store's rows; `findInArchive` is always empty and `purgeArchive` a no-op (no archive) |
| `InMemoryEntityLocker` | `EntityLocker` with TTL and a fencing token, matching the Redis locker's semantics |
| `InMemoryConnectionSupplier` | Stub whose `get()`/`release()` throw — exists only to satisfy DI wiring that expects the `ConnectionSupplier` port |

All classes accept an optional `Clock`, so the testkit's
`SettableClock` can drive time-dependent behaviour (retry eligibility,
TTL expiry, dead-worker detection) deterministically.

## When to use it

- **Spring tests** that need the outbox without a database.
- **Plain-Java tests** — usually indirectly, through the
  [testkit](event-outboxer-testkit.md), which wires these classes for you.
- **Never in production**, and never as a dev-profile convenience: it
  does not participate in transactions and loses everything on
  restart — the two failures this library exists to prevent.

For integration tests that must exercise real transactions and SQL,
use `@SpringBootTest` + Testcontainers with the
[PostgreSQL adapter](event-outboxer-storage-postgres.md) instead
(see [TESTING.md](../TESTING.md#interop-with-springboottest)).

## How to use it

### In a Spring test

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-storage-inmemory</artifactId>
    <scope>test</scope>
</dependency>
```

```java
@SpringBootTest
@Import(OutboxInMemoryTestConfiguration.class)   // ships in the starter
class MyOutboxTest {
    @Autowired OutboxEventPublisher publisher;
    // ...
}
```

`OutboxInMemoryTestConfiguration` is a plain `@Configuration` in the
[starter](event-outboxer-spring-boot-starter.md) (never
auto-configured — the `@Import` is the explicit opt-in ADR-0020
demands). It registers `InMemoryEventStore`, `InMemoryWorkerRegistry`,
`InMemoryConnectionSupplier` and `InMemoryOutboxAdmin`, each backing
off to any user-defined bean of the same port.

### In a plain-Java test

Prefer the [testkit](event-outboxer-testkit.md)'s
`OutboxTestContext`, which wires everything. Direct construction works
too:

```java
SettableClock clock = SettableClock.atEpoch();
InMemoryEventStore store = new InMemoryEventStore(clock);
InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry(clock);
```

### As a contract-test reference

Adapter authors: the in-repo tests
(`InMemoryEventStoreTest extends AbstractEventStoreContractTest`,
etc.) show exactly how to hook a new adapter into the SPI contract
tests — see [event-outboxer-spi](event-outboxer-spi.md#the-contract-test-kit).

## Related

- [ADR-0020](../adr/0020-no-inmemory-storage-in-production.md) — why there is no in-memory production storage.
- [TESTING.md](../TESTING.md) — the full testing guide.
- [event-outboxer-testkit](event-outboxer-testkit.md) — the higher-level test fixture built on this module.
