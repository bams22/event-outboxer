# Configuration

Full reference for `application.yml` used by
`event-outboxer-spring-boot-starter`. Every property below is bound by
`OutboxProperties` (prefix `event-outboxer`); defaults shown are the
values the starter actually applies.

## Contents

1. [Quick start](#quick-start)
2. [Full property tree](#full-property-tree)
3. [Section reference](#section-reference)
4. [Per-type override (thin merge)](#per-type-override-thin-merge)
5. [Invariant validation](#invariant-validation)
6. [Overriding through Java code](#overriding-through-java-code)

---

## Quick start

Minimal configuration for PostgreSQL:

```yaml
event-outboxer:
  storage:
    type: postgres      # default is inmemory — set explicitly for production

spring:
  flyway:
    locations:
      - classpath:db/migration
      - classpath:db/migration/outbox/core
```

Everything else comes from the defaults. To add the Redis/KeyDB entity
lock, set `event-outboxer.lock.type: redis` and provide a Lettuce
`StatefulRedisConnection<String, String>` bean (see
[`event-outboxer.lock.*`](#event-outboxerlock)).

---

## Full property tree

```yaml
event-outboxer:
  enabled: true                      # master switch, default=true

  worker:
    id: null                         # override WorkerId; null = {hostname}-{pid}-{uuid8}
    host: null                       # override hostname; null = resolved automatically
    metadata:                        # arbitrary Map<String,String> → event_outboxer.workers.metadata JSONB
      app: my-service
      version: ${git.commit.sha:unknown}

  publisher:
    no-transaction-policy: FAIL      # FAIL | IGNORE

  storage:
    type: postgres                   # REQUIRED — no default, no in-memory option (ADR-0020)
    # schema is shared between the adapter (SQL) and the classpath
    # migrations (Flyway ${eventOutboxerSchema} placeholder). Default
    # name is specific to avoid conflicts with other libraries.
    schema: event_outboxer
    table-prefix: ""                 # optional table-name prefix (event_outboxer.<prefix>events)
    archive-enabled: false           # move successful events to the archive table
    metrics-cache-ttl: 30s           # TTL of the metricsSnapshot() cache

  lock:
    type: noop                       # noop (default) | postgres | redis
    key-prefix: "outbox:lock:"

  cache:
    type: memory                     # memory (default) | redis | noop
    redis:
      key-prefix: "outbox:metrics:"  # shared key namespace when type=redis

  event-types:
    defaults:
      poll-min-interval: 500ms       # floor of the adaptive poll interval
      poll-max-interval: 10s         # ceiling of the adaptive poll interval
      poll-multiplier: 1.5           # growth factor after an empty poll; must be > 1.0
      claim-batch-size: 10           # events claimed per poll
      handler-pool-size: 3           # fixed per-type thread pool (core == max, no scaling)
      handler-queue-capacity: 100    # bounded queue; 0 = synchronous handoff (fail fast)
      handler-max-runtime: 5m        # watchdog threshold for a stuck handler
      lock-ttl: 10m                  # entity-lock TTL; must be >= handler-max-runtime (2x recommended)
    overrides:                       # thin merge: set only the fields you change
      SEND_EMAIL:
        handler-pool-size: 20
        poll-min-interval: 2s
      UPDATE_CACHE:
        handler-pool-size: 30
        poll-min-interval: 500ms
        handler-max-runtime: 1m

  dispatcher:
    unknown-handler-policy: SKIP     # SKIP (default) | DISABLE | FAIL
    unknown-handler-retry-delay: 1m  # reschedule delay when policy=SKIP
    lock-busy-retry-delay: 1s        # reschedule delay when the entity lock is busy
    dispatch-rejected-retry-delay: 1s # reschedule delay when the handler executor is saturated
    finalize-batching: true          # group-commit batching of finalize statements
    finalize-batch-max-size: 128     # cap on rows per flushed finalize statement

  maintenance:
    heartbeat-interval: 5s           # how often the worker refreshes event_outboxer.workers
    dead-threshold: 30s              # heartbeat silence before a worker counts as dead
    orphan-recovery-interval: 30s    # orphan-recovery task period
    watchdog-interval: 10s           # stuck-handler watchdog period
    reclaim-batch-size: 50           # max dead workers processed per orphan-recovery pass
    shutdown-timeout: 30s            # max wait for in-flight handlers on shutdown
    stale-claim-threshold: null      # null = derived: 2 × max handler-max-runtime
    stale-claim-sweep-interval: 5m   # stale-claim sweeper period

  handler-executor:
    type: platform                   # platform | virtual

  metrics:
    # Prefix applied to every Micrometer counter/timer/summary. Default
    # chosen to avoid clashing with other libraries that publish outbox.*.
    prefix: event_outboxer

  health:
    # Merge the outbox indicator into these Actuator health groups. Default
    # empty = no influence on /actuator/health/liveness or /readiness.
    # For k8s rolling restart: [readiness] is the recommended minimum.
    probe-groups: []

  retention:
    archive-older-than: null         # e.g. 30d; null (default) = archive retention off
    disabled-older-than: null        # e.g. 90d; null (default) = DISABLED retention off
    batch-size: 1000                 # rows per DELETE; a pass loops until a short batch
    interval: 1h                     # delay between retention passes

  admin:
    rest:                            # requires the event-outboxer-admin-rest module
      enabled: false                 # write-capable HTTP surface — strictly opt-in
      base-path: /outbox-admin
      required-authority: OUTBOX_ADMIN
      enforce-authority: true        # fail startup if @PreAuthorize would be silently ignored
```

---

## Section reference

### `event-outboxer.enabled`

Master switch. `false` — the library does not activate, no beans, no
pollers. Useful for dev/staging environments where the outbox is not
needed.

### `event-outboxer.worker.*`

- `id` — override for the autogenerated WorkerId. By default it is
  generated as `{hostname}-{pid}-{uuid8}`
  (e.g. `api-srv-01-4817-a3f2b1c9`). Typically untouched.
- `host` — explicit hostname stored in the worker registry; resolved
  automatically when unset.
- `metadata` — an arbitrary `Map<String,String>` written to
  `event_outboxer.workers.metadata JSONB` on registration. Useful for
  debugging: application version, git-sha, environment, image tag.

### `event-outboxer.publisher.no-transaction-policy`

Behavior of `OutboxEventPublisher.publish()` when called outside an
active transaction:

- `FAIL` (default) — `NoTransactionException` is thrown. Safe: prevents
  accidentally publishing without atomicity.
- `IGNORE` — writes without a surrounding transaction. Unsafe, for
  tests only.

### `event-outboxer.storage.*`

Storage adapter settings.

- `type` — **required, no default** (ADR-0020). The only value is
  `postgres` (requires a `DataSource` bean and the
  `event-outboxer-storage-postgres` dependency). There is deliberately
  no in-memory option: a silently non-durable outbox would not
  participate in your transactions and would lose events on restart —
  the exact failure this library exists to prevent. An unconfigured
  outbox fails at startup with an actionable message. For tests, see
  [Testing without a database](#testing-without-a-database).
- `schema` — schema name. **Default: `event_outboxer`** — a specific
  name chosen to avoid clashing with other libraries or application
  tables in a shared database. The value is propagated into both the
  adapter's SQL and the Flyway placeholder `${eventOutboxerSchema}`
  used by the classpath migrations, so changing it once updates both.
- `table-prefix` — optional table prefix (e.g. `v1_` →
  `event_outboxer.v1_events`).
- `archive-enabled` — enables archiving of successful events (requires
  the archive migration; see ADR-0008).
- `metrics-cache-ttl` — TTL applied by the default in-memory cache and
  (when `event-outboxer.cache.type=redis`) as the PX expire on the Redis key.
  Ignored when `event-outboxer.cache.type=noop` or a custom
  `@Bean MetricsSnapshotCache` takes over.

### `event-outboxer.lock.*`

`EntityLocker` selection. There is no classpath auto-detection — the
default is `noop` and other backends are opt-in:

- `type: noop` (**default**) — no business-key locking.
- `type: postgres` — `pg_advisory_lock`-based locker; requires
  `event-outboxer-lock-postgres` on the classpath and a `DataSource`
  bean.
- `type: redis` — Redis/KeyDB locker; requires
  `event-outboxer-lock-redis` on the classpath and a user-provided
  Lettuce `StatefulRedisConnection<String, String>` bean (the starter
  does not manage Redis connections itself).
- `key-prefix` — prefix for lock keys, default `outbox:lock:`.

### `event-outboxer.cache.*`

Backs `EventStore.metricsSnapshot()` caching. See
[docs/STORAGE.md §Pluggable metrics cache](STORAGE.md#pluggable-metrics-cache)
for the motivation (consistent snapshot across pods) and the full
Redis wiring recipe.

- `type` — one of:
  - `memory` (default) — per-JVM `AtomicReference` TTL cache, keyed off
    `event-outboxer.storage.metrics-cache-ttl`.
  - `noop` — caching disabled; every `metricsSnapshot()` call hits the
    database. Useful for tests that need live state.
  - `redis` — shared Redis/KeyDB-backed cache; requires
    `event-outboxer-cache-redis` on the classpath and a
    `StatefulRedisConnection<String, String>` bean.
- `redis.key-prefix` — prefix prepended to the cache key when
  `type=redis`. Default `outbox:metrics:`; the cache writes a single
  key `<key-prefix>snapshot`.

A user-defined `@Bean MetricsSnapshotCache` wins over every autowired
variant regardless of `type`.

### `event-outboxer.event-types.defaults` / `event-outboxer.event-types.overrides.<type>`

Per-event-type engine settings. Defaults apply to every type;
per-type overrides adjust individual fields (see
[thin merge](#per-type-override-thin-merge)).

- `poll-min-interval` / `poll-max-interval` / `poll-multiplier` — the
  adaptive poller starts at the min interval, multiplies the wait by
  `poll-multiplier` after every empty poll, and caps it at the max
  interval; any non-empty poll resets the wait to the minimum.
  Note: these intervals bound the pickup latency only for events
  published by *other* JVMs and for delayed events (`runAt` in the
  future). Events published in this JVM wake their poller right after
  the publishing transaction commits, so same-JVM latency is
  milliseconds regardless of the poll intervals (ADR-0006 amendment).
- `claim-batch-size` — how many events to claim per poll.
- `handler-pool-size`, `handler-queue-capacity` — fixed-size
  executor per event type (`core == max`, no scaling). Their sum is the
  type's **in-flight budget**: the poller claims at most
  `min(claim-batch-size, free capacity)` per poll and stops claiming
  entirely while the budget is exhausted, resuming the moment a handler
  slot frees. A full claimed batch triggers an immediate re-poll, so
  sustained throughput is bounded by the pool and the database — not by
  `claim-batch-size / poll-min-interval`. For
  `handler-executor.type: virtual` the same sum acts as a soft
  in-flight cap (the executor itself is unbounded). A zero
  `handler-queue-capacity` makes dispatch a synchronous handoff. A
  rejected dispatch (rare capacity race) is not lost: the event is
  released back to `PENDING` (without consuming an attempt) and retried
  after `dispatcher.dispatch-rejected-retry-delay`.
- `handler-max-runtime` — watchdog threshold. A handler running longer
  is force-reclaimed (see ADR-0005).
- `lock-ttl` — entity-lock TTL passed to `EntityLocker.tryLock()`.
  **Must be `>= handler-max-runtime`** (validated at startup): for
  TTL-honouring lockers (Redis) a shorter TTL would let the lock
  expire while a legitimate handler still runs, breaking per-key
  serialization. Default 10m = 2 × the default handler budget —
  keep the 2× margin (the TTL is the crash-release mechanism, and the
  margin covers a zombie handler that outlives its force-reclaimed
  claim). Raising `handler-max-runtime` above `lock-ttl` fails
  startup until `lock-ttl` is raised too. See the ADR-0012 amendment
  for the per-backend guarantee table; note that
  `lock.type=postgres` holds one pooled connection per held lock —
  the starter warns when `Σ handler-pool-size >=
  spring.datasource.hikari.maximum-pool-size` (self-deadlock risk).

### `event-outboxer.dispatcher.*`

Cross-type dispatcher knobs.

- `unknown-handler-policy` — what to do with a claimed event whose type
  has no registered handler (see ADR-0013): `SKIP` (default —
  reschedule after `unknown-handler-retry-delay` without consuming an
  attempt), `DISABLE` (move to `DISABLED`), `FAIL` (leave the row
  `PROCESSING` as a visible poison-pill marker; it is released back to
  `PENDING` on engine shutdown).
- `unknown-handler-retry-delay` — reschedule delay for `SKIP`.
- `lock-busy-retry-delay` — reschedule delay when the entity lock is
  busy or errored. Lock contention does not consume the attempts
  budget.
- `dispatch-rejected-retry-delay` — reschedule delay when the per-type
  handler executor rejects a dispatch (pool and queue saturated).
  Backpressure does not consume the attempts budget either.
- `finalize-batching` — group-commit batching of `markProcessed` /
  `markForRetry` statements (ADR-0014, batch form): concurrent
  finalizations coalesce into one multi-row statement, cutting finalize
  round-trips on hot types up to ~batch-size×. The batch forms while
  the previous statement is in flight — no timers, no added latency; an
  idle engine degrades to plain single-row calls. `true` by default;
  disable only as a kill-switch.
- `finalize-batch-max-size` — cap on rows per flushed finalize
  statement (default 128).

### `event-outboxer.maintenance.*`

Maintenance-process parameters.

- `heartbeat-interval` — how often the worker refreshes its
  `event_outboxer.workers` row. The PostgreSQL adapter stamps the
  database clock (`now()`), so worker liveness is immune to
  application-JVM clock skew.
- `dead-threshold` — heartbeat silence before a worker is considered
  dead. **Invariant**: `dead-threshold >= 3 × heartbeat-interval`
  (protection against GC-stall false positives).
- `orphan-recovery-interval` — period of `OrphanRecoveryTask`.
- `watchdog-interval` — period of `WatchdogTask` (also used by the
  engine crash detector).
- `reclaim-batch-size` — maximum number of dead workers processed per
  orphan-recovery pass.
- `shutdown-timeout` — maximum wait for in-flight handlers during
  graceful shutdown. Events still claimed after the drain (queued or
  interrupted) are released back to `PENDING` before the worker
  deregisters. See
  [docs/ARCHITECTURE.md §SmartLifecycle phases](ARCHITECTURE.md#3-smartlifecycle-phases)
  for the drain sequence.
- `stale-claim-threshold` — age of a `PROCESSING` claim before the
  stale-claim sweeper returns it to `PENDING` (last line of defence
  for rows invisible to the watchdog and orphan recovery). Default:
  derived as 2 × the largest per-type `handler-max-runtime`. An
  explicit value must exceed every `handler-max-runtime` — validated
  at startup. Heterogeneous fleets (a rolling deploy raising
  `handler-max-runtime`) should set it explicitly with headroom.
- `stale-claim-sweep-interval` — cadence of the sweeper (default 5m).

Note on `handler-max-runtime` semantics: since the in-flight bracket
covers the whole dispatch, the budget includes payload deserialization
and entity-lock acquisition, not just `handler.handle()`.

> See [docs/OBSERVABILITY.md](OBSERVABILITY.md) for what these knobs
> look like from the outside — the health endpoint, the Micrometer
> metric list and five troubleshooting recipes.

### `event-outboxer.handler-executor.type`

- `platform` (default) — `ThreadPoolTaskExecutor` on platform threads.
- `virtual` — virtual-thread-per-task `ExecutorService` wrapped in
  `ContextPropagatingExecutorService`. Requires JDK 21+ at runtime
  (baseline is Java 17, virtual-thread APIs invoked via reflection);
  JDK 25+ additionally eliminates `synchronized` pinning via JEP 491,
  making the variant safe with JDBC drivers.

### `event-outboxer.metrics.*`

Micrometer listener settings. `MicrometerOutboxListener` registers
automatically when Micrometer and the `event-outboxer-metrics-micrometer`
module are on the classpath and a `MeterRegistry` bean exists.

- `prefix` — prefix applied to every counter / timer / summary
  registered by `MicrometerOutboxListener`. **Default:
  `event_outboxer`** — a specific name chosen to avoid clashing with
  other libraries that publish `outbox.*` metrics. Override when
  multiple outbox instances share a registry or when an organisation
  requires a different namespace. See [docs/OBSERVABILITY.md](OBSERVABILITY.md)
  for the full metric catalogue.

### `event-outboxer.health.*`

Spring Boot Actuator integration.

- `probe-groups` — list of Actuator health groups into which the
  `outbox` indicator is merged. Typical values: `readiness`,
  `liveness`. **Default: empty** (the indicator lives only at
  `/actuator/health/outbox`; probes are unaffected). When set, an
  `EnvironmentPostProcessor` appends `outbox` to
  `management.endpoint.health.group.<name>.include` for each listed
  group, preserving your existing includes and the default
  `<name>State` contributor. See [docs/OBSERVABILITY.md §Kubernetes probes](OBSERVABILITY.md#kubernetes-probes)
  for the tradeoffs between probe-driven pod lifecycle and
  metric-driven alerting.

### `event-outboxer.retention.*`

Optional cleanup of the archive table and of accumulated `DISABLED`
events (ADR-0019), executed by a maintenance task on the engine's
scheduler. **Both thresholds default to off** — deleting data is never
a surprise default; enable with one line, e.g.
`retention.archive-older-than: 30d`. `disabled-older-than` ages by
`created_at` (the schema does not record the moment of disabling).
Requires the storage adapter's `OutboxAdmin` (wired automatically by
the starter).

### `event-outboxer.admin.rest.*` and the admin modules

Operational surface over the `OutboxAdmin` SPI port (ADR-0019): list
events by status with keyset pagination, look events up in the
archive, re-enable `DISABLED` events (single or bulk, with a fresh
attempts budget), purge old rows. Two interchangeable surfaces, each
activated by adding its module next to the starter:

- **`event-outboxer-admin-actuator`** — Actuator endpoint
  `outboxadmin`. Not exposed by default; expose with
  `management.endpoints.web.exposure.include=outboxadmin` and secure
  it like any other Actuator endpoint. No `event-outboxer.*`
  properties of its own.
- **`event-outboxer-admin-rest`** — REST controller under
  `base-path`. `enabled` defaults to `false`. Every operation requires
  the authority named by `required-authority` on the authenticated
  principal, enforced via `@PreAuthorize` + Spring **method
  security**. Security posture:
  - no Spring Security on the classpath → the API runs unprotected
    (accepted for security-less apps);
  - Spring Security present but `@EnableMethodSecurity` missing →
    **startup fails** with an actionable message, because the
    annotation would otherwise be silently ignored;
    `enforce-authority: false` is the explicit opt-out.

### Testing without a database

In-memory storage exists solely as test infrastructure and is
unreachable through `event-outboxer.*` properties (ADR-0020). In
Spring tests, opt in explicitly:

```java
@SpringBootTest
@Import(OutboxInMemoryTestConfiguration.class)
class MyOutboxTest { ... }
```

with `event-outboxer-storage-inmemory` on the test classpath:

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-storage-inmemory</artifactId>
    <scope>test</scope>
</dependency>
```

For plain-Java (non-Spring) tests use the testkit's
`OutboxTestContext`, which wires the in-memory store directly.

### Serialization

The MVP ships Jackson only (see ADR-0011); it activates automatically
when Jackson is on the classpath. There are no
`event-outboxer.serializer.*` properties — customise serialization by
providing an `ObjectMapper` bean named `outboxObjectMapper` (falls back
to the primary `ObjectMapper`, then to library defaults) or by
registering your own `@Bean EventSerializer`.

#### DTO evolution and rolling deploys

The library's default `ObjectMapper` is deliberately
evolution-friendly (`FAIL_ON_UNKNOWN_PROPERTIES` disabled): during a
rolling deploy, mixed-version replicas read each other's payloads, and
strictness there would disable events instead of processing them.
With the defaults:

- **Adding a DTO field is safe in both directions.** An outdated
  replica ignores the unknown field; an updated replica reads an old
  payload with the field absent (defaults / `null` — give new fields
  a default value or a nullable type).
- **Removing a field is safe** — old payloads simply carry an ignored
  extra property.
- **Renaming** — use `@JsonAlias("oldName")` on the new component for
  one release, then drop it once pre-rename events are drained.
- **Type changes are not safe**; publish a new event type instead.

If deserialization does fail (a truly incompatible change or corrupt
data), the event is **not** lost: the failure routes through the
`FailureHandler` chain — retried with backoff, `DISABLED` only after
the chain's attempt budget (10 by default) is exhausted, with
`OutboxListener.onEventSerializationError` fired on every failed
attempt. Strict deserialization can be restored by supplying a strict
`@Bean("outboxObjectMapper")`.

### Failure handling

Retry/backoff policy is configured **in Java, not YAML**: provide a
`@Bean` `FailureHandler` (qualifier `outboxDefaultFailureHandler` for
the global default, or the `outboxPerTypeFailureHandlers` map for
per-type chains), or override `EventHandler.failureHandler()` on a
specific handler. The default chain is
`FailureHandlers.defaults()` — logging + max-attempts (10, then
DISABLE) + exponential backoff. See ADR-0007 and
[Overriding through Java code](#overriding-through-java-code).

---

## Per-type override (thin merge)

`event-outboxer.event-types.overrides.<type>` overrides
`event-outboxer.event-types.defaults` **field by field,
independently**. Unset fields in `defaults` fall back to the library
defaults (`EventTypeConfig.defaults()`). For example:

```yaml
event-outboxer:
  event-types:
    defaults:
      poll-min-interval: 250ms
      claim-batch-size: 42
    overrides:
      SEND_EMAIL:
        handler-pool-size: 20   # ONLY this is overridden
```

Effective configuration for `SEND_EMAIL`:
- `handler-pool-size: 20` (override)
- `poll-min-interval: 250ms` (from defaults)
- `claim-batch-size: 42` (from defaults)
- everything else — library defaults.

The merge is performed by the starter when it maps `OutboxProperties`
to the core `EventTypeConfig` objects.

---

## Invariant validation

Violations fail fast at startup — the configuration records validate
their invariants in their constructors, so a bad value aborts context
refresh:

| Rule | Where | Why |
|---|---|---|
| `dead-threshold >= 3 × heartbeat-interval` | `MaintenanceConfig` | Protect against GC-stall false positives |
| `poll-min-interval > 0`, `poll-max-interval >= poll-min-interval` | `EventTypeConfig` | Adaptive backoff needs a sane range |
| `poll-multiplier > 1.0` | `EventTypeConfig` | Adaptive backoff needs growth |
| `claim-batch-size > 0`, `handler-pool-size > 0`, `handler-queue-capacity >= 0` | `EventTypeConfig` | Pool is fixed-size and bounded |
| `handler-max-runtime > 0`, `lock-ttl > 0` | `EventTypeConfig` | Sanity |
| retry delays not negative | `DispatcherConfig` | Sanity |

---

## Overriding through Java code

Property binding is not enough for complex cases. Override via Spring
beans:

### Custom ObjectMapper for serialization

```java
@Bean
public ObjectMapper outboxObjectMapper() {
    return JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .addModule(new KotlinModule.Builder().build())
        .build();
}
```

### Custom TaskDecorator for context propagation

```java
@Bean
public TaskDecorator myOutboxTaskDecorator() {
    return runnable -> {
        String tenantId = TenantContext.current();
        return () -> {
            TenantContext.set(tenantId);
            try { runnable.run(); }
            finally { TenantContext.clear(); }
        };
    };
}
```

### Custom FailureHandler per type

```java
@Bean
public FailureHandler<SendEmailPayload> sendEmailFailureHandler() {
    return FailureHandlers.<SendEmailPayload>builder()
        .withLogging(LogLevel.WARN)
        .withMaxAttempts(5, DISABLE)
        .withExponentialBackoff(
            Duration.ofSeconds(30), 2.0, Duration.ofHours(2), 0.2)
        .build();
}
```

### Custom OutboxListener

```java
@Component
public class AuditListener implements OutboxListener {
    private final AuditLogRepository repo;

    @Override
    public void onEventDisabled(EventDisabledInfo info) {
        repo.save(new AuditEntry(
            info.eventId(), "DISABLED", info.reason(), now()));
    }
}
```

### Custom EventHandler.failureHandler()

```java
@Component
public class ValidationHandler implements EventHandler<ValidationPayload> {
    @Override
    public FailureHandler<ValidationPayload> failureHandler() {
        // validation is not retried
        return new NoRetryFailureHandler<>();
    }
    // ...
}
```

---

## Related documents

- [docs/ARCHITECTURE.md](ARCHITECTURE.md) — architectural overview.
- [docs/adr/README.md](adr/README.md) — rationale.
