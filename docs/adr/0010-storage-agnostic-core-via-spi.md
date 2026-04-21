# ADR-0010: Storage-agnostic core via SPI

## Status

Accepted

## Date

2026-04-19

## Context

From the outset we fixed the requirement: **storage must be abstract and
replaceable**. This delivers:
- Support for different databases (PostgreSQL in MVP, potentially MongoDB,
  DynamoDB, etc.).
- Testability: an in-memory implementation for unit tests without
  Testcontainers.
- Flexibility: users can write their own adapter for a non-standard storage
  (e.g. S3 for the archive, Redis for in-flight events).

This decision shapes the whole core engine design — it must work only
against abstract interfaces.

## Alternatives considered

- **A. Storage-specific core**: PG queries directly inside the engine code.
- **B. Pluggable via SPI**: ports in a dedicated module, implementations in
  separate modules.

## Decision

**Option B was chosen**: the core interacts with storage only through SPI
ports.

### SPI ports in `event-outboxer-spi`

```java
public interface EventStore {
    void save(PendingEvent event);
    void saveAll(Collection<PendingEvent> events);
    List<ClaimedEvent> claim(ClaimRequest request);
    boolean markProcessed(UUID eventId, long claimedVersion, WorkerId workerId);
    boolean markForRetry(...);
    boolean markDisabled(...);
    boolean forceReclaim(...);
    int reclaimOrphans(Collection<WorkerId> deadWorkerIds);
    Optional<Event> findById(UUID eventId);
    OutboxMetricsSnapshot metricsSnapshot();
}

public interface WorkerRegistry {
    void register(WorkerInfo info);
    void heartbeat(WorkerId workerId);
    void markGracefulStop(WorkerId workerId);
    void deregister(WorkerId workerId);
    List<WorkerId> findDead(Duration deadThreshold, int limit);
    void removeDead(Collection<WorkerId> workerIds);
    // ...
}

public interface EntityLocker {
    Optional<LockHandle> tryLock(String key, Duration ttl);
    // ... EntityLocker.NOOP singleton
}

public interface EventSerializer {
    String serialize(Object payload);
    <T> T deserialize(String data, Class<T> type);
}

public interface Clock {
    Instant now();
    static Clock system() { return Instant::now; }
}
```

### Implementations in separate modules

- `event-outboxer-storage-postgres` — `EventStore` + `WorkerRegistry` for
  PG.
- `event-outboxer-storage-inmemory` — for tests.
- `event-outboxer-lock-postgres` — `EntityLocker` via
  `pg_advisory_xact_lock`.
- `event-outboxer-lock-redis` — via Redis/KeyDB.
- `event-outboxer-serializer-jackson` — Jackson JSON (see ADR-0011).

### Abstraction test

For every port: "can I write a MongoDB implementation without changing the
interface?" If not, the interface leaks storage-specific assumptions.

**Bad** (leaking PG specifics):
```java
List<ClaimedEvent> selectForUpdateSkipLocked(String eventType, int limit);
```

**Good** (storage-agnostic):
```java
List<ClaimedEvent> claim(ClaimRequest request);
```

The PG adapter uses `FOR UPDATE SKIP LOCKED`, the MongoDB one
`findAndModify`, the in-memory one a CAS loop. The port does not know.

## Rationale

- **Flexibility**: supporting a new DB requires a new module, not core
  changes.
- **Testability**: core unit tests run against `InMemoryEventStore` without
  a database. Fast, deterministic tests.
- **Clean boundaries**: adapter authors know exactly what they must
  implement (SPI) and what they must not touch (core).
- **Follows hexagonal architecture**: ports and adapters — standard
  pattern for infrastructure independence.
- **db-scheduler does the same thing**: a proven approach in the Java
  ecosystem.

## Consequences

### For users

- Storage choice is a matter of depending on one of the
  `event-outboxer-storage-*` modules.
- Custom adapters are possible by implementing the SPI ports.
- For tests use `event-outboxer-storage-inmemory` +
  `event-outboxer-testkit` — no Testcontainers needed.

### For maintainers

- **The core MUST NOT contain** storage-specific code (SQL, JDBC types,
  Mongo query objects). Enforced by module dependencies.
- SPI is a stable API. Changes require migration across all adapters.
- The PG adapter is **one of the implementations**, not privileged. It can
  hypothetically be removed without core changes (though it is the primary
  MVP backend).

### Positive consequences

- Clean architecture.
- Extensible without a fork.
- Easy to test.
- Core compiles with a minimum classpath.

### Negative consequences

- One more module (`-spi`) — boundaries matter more than a single extra
  artifact.
- A small method-dispatch overhead through interfaces (ignored by JIT).
- SPI evolution is harder — requires default methods and backward-compat
  rules.

## Related decisions

- [ADR-0002](0002-participate-in-client-transaction.md) — Spring
  integration lives in the starter only, not the core.
- [ADR-0009](0009-spring-task-executor-in-starter.md) — Spring
  `TaskExecutor` lives in the starter only.
- [ADR-0016](0016-maven-module-structure.md) — module layout.
