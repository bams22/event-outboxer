# event-outboxer-lock-redis

Redis/KeyDB `EntityLocker` over Lettuce: `SET NX PX` acquire with a
per-acquisition fencing token and a Lua compare-and-delete release —
the single-instance Redlock recipe.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-lock-redis` |
| Java package | `io.github.bams22.outboxer.lock.redis` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `io.lettuce:lettuce-core` (6.x) |
| Requires | Redis 7+ / KeyDB 6+, and a user-provided `StatefulRedisConnection<String, String>` bean |
| Enable with | `event-outboxer.lock.type: redis` |

## What it does

**`RedisEntityLocker`**:

- **Acquire** — `SET <keyPrefix><key> <token> NX PX <ttlMillis>`; a
  `null` reply means busy → `Optional.empty()`. The token is a random
  UUID per acquisition.
- **Release** — a Lua script deletes the key **only if it still holds
  this acquisition's token**, so a zombie handler whose TTL expired
  can never free the next holder's lock. A `0` result (expired /
  taken over) is a debug-logged no-op.
- **TTL always honoured** — exclusion holds until `min(close(), ttl)`;
  after a crash the key self-frees at TTL. There is no renewal: a
  handler outliving the TTL can overlap with the next holder, which is
  why the engine enforces `lock-ttl ≥ handler-max-runtime` (default
  2×). Full fencing at the business resource is out of scope.
- Keys are the raw handler lock keys under a namespace prefix
  (default `outbox:lock:`) — no hashing, no length limit.
- The adapter does **not** own the connection: you create and close
  the `RedisClient` / `StatefulRedisConnection`; the release path
  rides the dedicated Lettuce connection, so it is immune to JDBC
  pool saturation (unlike the PG lockers).

Failure semantics: Redis unreachable on acquire →
`LockAcquisitionException` → the dispatcher reschedules the event
after `lock-busy-retry-delay` *without consuming an attempt*;
unreachable on release → absorbed (`onLockReleaseFailed`), key frees
at TTL.

## When to use it

- You already run Redis/KeyDB and want the lock domain decoupled from
  the database — multi-region setups, several services sharing one
  lock namespace, or outbox databases behind aggressive pooling.
- You want lock release latency independent of the JDBC pool.

Prefer [postgres-lease](event-outboxer-lock-postgres-lease.md) when
PostgreSQL is your only infrastructure — same guarantee level
(`min(close, ttl)`), one less failure domain.

## How to configure it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-lock-redis</artifactId>
</dependency>
```

```yaml
event-outboxer:
  lock:
    type: redis
    key-prefix: "outbox:lock:"   # default
```

The starter does **not** manage Redis connections — provide the bean
yourself (it can be shared with
[`event-outboxer-cache-redis`](event-outboxer-cache-redis.md)):

```java
@Bean(destroyMethod = "shutdown")
public RedisClient redisClient() {
    return RedisClient.create("redis://localhost:6379");
}

@Bean(destroyMethod = "close")
public StatefulRedisConnection<String, String> redisConnection(RedisClient client) {
    return client.connect();
}
```

The lock TTL passed to `tryLock` is the per-type
`event-outboxer.event-types.*.lock-ttl` (default 10 m).

### Without Spring

```java
EntityLocker locker = new RedisEntityLocker(connection);            // default prefix
// or: new RedisEntityLocker(connection, "myapp:outbox:lock:")
new OutboxEngineBuilder().entityLocker(locker)/*...*/.build();
```

## Related

- [event-outboxer-lock-postgres-lease](event-outboxer-lock-postgres-lease.md) — guarantee comparison across all lockers.
- ADRs: [0012](../adr/0012-extract-lock-key-on-handler.md), [0022](../adr/0022-lease-table-postgres-entity-locker.md) (§guarantee table).
