# event-outboxer-cache-redis

A Redis/KeyDB-backed `MetricsSnapshotCache`: shares the
`EventStore.metricsSnapshot()` result across all replicas so health
endpoints and backlog gauges report **one consistent view per TTL
window** instead of N per-JVM caches refreshing on their own rhythm.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-cache-redis` |
| Java package | `io.github.bams22.outboxer.cache.redis` |
| Depends on | [`event-outboxer-spi`](event-outboxer-spi.md), `io.lettuce:lettuce-core`, `jackson-databind` + `jsr310` |
| Enable with | `event-outboxer.cache.type: redis` + a `StatefulRedisConnection<String, String>` — starter-managed via `event-outboxer.redis.*` (ADR-0027) or user-provided |

## Why it exists

`metricsSnapshot()` is the aggregate query feeding
`/actuator/health/outbox` and the Micrometer backlog gauges
(pending/processing/disabled per type, oldest ages). By default each
JVM caches it independently for
`event-outboxer.storage.metrics-cache-ttl` (30 s): in a fleet, two
scrapes seconds apart hit different replicas with differently-aged
caches, so dashboards look like they are flapping — and every pod
still runs its own aggregate query. A shared Redis cache collapses
the fleet onto one snapshot per TTL window and divides the query load
by the replica count.

## What it does

**`LettuceMetricsSnapshotCache`**:

- One key for the whole deployment: `<key-prefix>snapshot` (default
  `outbox:metrics:snapshot`), written with `SET … PX <ttl>` — the TTL
  is enforced server-side, atomically with the write.
- Value: Jackson JSON of `OutboxMetricsSnapshot` (the default mapper
  adds `JavaTimeModule` for the `Instant` fields; a custom
  `ObjectMapper` can be passed via the full constructor).
- **Fail-safe by design**: any Lettuce or deserialization error is
  logged at WARN and reported as a cache miss — callers recompute from
  the database, so a Redis outage costs the shared view, never the
  health probe.
- Does not own the connection — the caller creates and closes the
  `RedisClient` / `StatefulRedisConnection`.

## When to use it

- Multi-replica deployments where consistent health/metrics across
  pods matters, or where per-JVM snapshot queries measurably load the
  database.
- Single-pod deployments do not need it — keep the default
  `cache.type: memory`.
- `cache.type: noop` disables caching entirely (every call hits the
  DB) — useful in tests that assert on live backlog state.

## How to configure it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-cache-redis</artifactId>
</dependency>
```

```yaml
event-outboxer:
  redis:
    uri: redis://localhost:6379       # starter-managed connection (ADR-0027)
  cache:
    type: redis
    redis:
      key-prefix: "outbox:metrics:"   # default; the cache writes <prefix>snapshot
  storage:
    metrics-cache-ttl: 30s            # default; becomes the PX expire on the key
```

With `event-outboxer.redis.uri` (or `.host`) set, the starter creates
and owns the Lettuce connection, shared with the
[Redis locker](event-outboxer-lock-redis.md). Alternatively bring your
own bean — it wins and makes `event-outboxer.redis.*` inert; with
several connection beans, mark the outbox one with
`@OutboxRedisConnection`:

```java
@Bean(destroyMethod = "close")
@OutboxRedisConnection   // needed only when several connection beans exist
public StatefulRedisConnection<String, String> redisConnection(RedisClient client) {
    return client.connect();
}
```

A user-defined `@Bean MetricsSnapshotCache` wins over every autowired
variant regardless of `cache.type`.

### Without Spring

```java
MetricsSnapshotCache cache =
    new LettuceMetricsSnapshotCache(connection, Duration.ofSeconds(30));

EventStore store = new PostgresEventStore(
    connectionSupplier, PostgresStorageProperties.defaults(), Clock.system(), cache);
```

## Related

- [STORAGE.md §Pluggable metrics cache](../STORAGE.md#pluggable-metrics-cache) — motivation and the full wiring recipe.
- [CONFIGURATION.md §event-outboxer.cache](../CONFIGURATION.md#event-outboxercache).
