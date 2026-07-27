# ADR-0010: Storage-agnostic core via SPI

## Status

Accepted (amended 2026-07-27: port inventory and signature block
synchronized with the shipped SPI)

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

The signature block below is kept in sync with the shipped SPI (last
synchronized 2026-07-27; the `.java` files remain the authority):

```java
public interface EventStore {
    boolean save(PendingEvent event);                       // false = coalesced away (ADR-0021)
    void saveAll(List<PendingEvent> events);                // rejects dedup-keyed events
    Optional<UUID> lockPendingByDedupKey(String eventType, String dedupKey); // ADR-0021 pin
    List<ClaimedEvent> claim(ClaimRequest request);
    boolean markProcessed(UUID id, WorkerId workerId, long claimedVersion);
    boolean markForRetry(UUID id, WorkerId workerId, long claimedVersion,
                         String reason, Instant runAt);
    default Set<UUID> markProcessedAll(List<ProcessedMark> marks, WorkerId workerId);
    default Set<UUID> markForRetryAll(List<RetryMark> marks, WorkerId workerId);
                                                            // batch finalize (ADR-0014)
    boolean markDisabled(UUID id, WorkerId workerId, long claimedVersion, String reason);
    boolean release(UUID id, WorkerId workerId, long claimedVersion,
                    String reason, Instant runAt);          // no attempts bump
    int releaseClaimed(WorkerId workerId, Instant now);     // shutdown sweep
    boolean forceReclaim(UUID id, WorkerId workerId, long claimedVersion, Instant runAt);
    int sweepStale(Duration olderThan, int limit);          // stale-claim sweeper
    int reclaimOrphans(List<WorkerId> deadWorkers, Instant now);
    Optional<Event> findById(UUID id);
    OutboxMetricsSnapshot metricsSnapshot();

    record ProcessedMark(UUID id, long claimedVersion) {}
    record RetryMark(UUID id, long claimedVersion, String reason, Instant runAt) {}
}

public interface WorkerRegistry {
    void register(WorkerInfo info);
    boolean heartbeat(WorkerId id, Instant at);             // false = row vanished
    void markGracefulStop(WorkerId id);
    void deregister(WorkerId id);
    List<WorkerInfo> findDead(Duration deadThreshold, int limit);
    void removeDead(List<WorkerId> ids);
    Optional<WorkerInfo> findById(WorkerId id);
    List<WorkerInfo> findAll();
}

public interface EntityLocker {
    Optional<LockHandle> tryLock(String key, Duration ttl);
    EntityLocker NOOP = new NoopEntityLocker();
}

public interface EventSerializer {
    String serialize(Object payload);
    <T> T deserialize(String payload, Class<T> type);
}

public interface Clock {
    Instant now();
    static Clock system() { return Instant::now; }
}
```

Ports added after this ADR was first written, defined in the same
module and following the same rules:

- `OutboxAdmin` — administrative surface (list/reenable/purge, keyset
  pagination via `AdminCursor`), see ADR-0019.
- `ConnectionSupplier` — how the PG adapter joins the caller's
  transaction, see ADR-0002.
- `MetricsSnapshotCache` — TTL cache for `metricsSnapshot()` (in-memory
  and Redis implementations).
- Supporting types: `ClaimRequest`, `OutboxMetricsSnapshot`,
  `ArchivedEvent`, `LockHandle`.

### Implementations in separate modules

- `event-outboxer-storage-postgres` — `EventStore` + `WorkerRegistry` +
  `OutboxAdmin` for PG.
- `event-outboxer-storage-inmemory` — test infrastructure only
  (ADR-0020): contract tests, `@Import` test configuration.
- `event-outboxer-lock-postgres` — `EntityLocker` for PostgreSQL
  (ADR-0012; originally session-scoped `pg_advisory_lock`, since
  ADR-0022 a lease table by default with advisory as opt-out).
- `event-outboxer-lock-redis` — `EntityLocker` via Redis/KeyDB
  (Lettuce).
- `event-outboxer-cache-redis` — `MetricsSnapshotCache` via Redis/KeyDB.
- `event-outboxer-serializer-jackson` — Jackson JSON (see ADR-0011).
- `event-outboxer-admin-actuator` / `event-outboxer-admin-rest` —
  surfaces over `OutboxAdmin` (ADR-0019).

The full 15-module layout lives in ADR-0016.

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
