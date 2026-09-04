# event-outboxer-lock-redis

Redis/KeyDB `EntityLocker` over Lettuce: `SET NX PX` acquire with a
per-acquisition fencing token and a Lua compare-and-delete release —
the single-instance Redlock recipe.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-lock-redis` |
| Java package | `io.github.bams22.outboxer.lock.redis` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `io.lettuce:lettuce-core` (6.x) |
| Requires | Redis 7+ / KeyDB 6+, and a `StatefulRedisConnection<String, String>` — starter-managed via `event-outboxer.redis.*` (ADR-0027) or user-provided; optionally a `StatefulRedisPubSubConnection<String, String>` for the lock-wait wake-up (starter-managed too) |
| Enable with | `event-outboxer.lock.type: redis` |

## What it does

**`RedisEntityLocker`**:

- **Acquire** — `SET <keyPrefix><key> <token> NX PX <ttlMillis>`; a
  `null` reply means busy → `Optional.empty()`. The token is a random
  UUID per acquisition.
- **Release** — a Lua script deletes the key **only if it still holds
  this acquisition's token**, so a zombie handler whose TTL expired
  can never free the next holder's lock, and then `PUBLISH`es the token
  on the key's channel `<keyPrefix>released:<key>` for waiters. A `0`
  result (expired / taken over) is a debug-logged no-op.
- **Bounded wait with a wake-up (ADR-0035)** — given a second, pub/sub
  connection, a handler thread that finds the key busy during its
  type's `lock-wait` subscribes to that channel and parks until the
  holder releases: the first waiter of a key subscribes, later ones
  share the subscription, the last one unsubscribes. A parked waiter
  still re-probes every 25 ms (`fallbackProbeInterval`) because
  pub/sub is at-most-once — a notification lost across a reconnect or
  a key that expired instead of being released must not cost the whole
  budget. Without a pub/sub connection the wait polls `SET NX PX`
  every 2–10 ms (the SPI default). `PUBLISH` is issued either way: one
  cheap command, and the waiters may sit in another JVM. The starter
  publishes how the waits ended as `event_outboxer.lock.wakeups{result}`
  (`notified` / `probed` / `exhausted` / `interrupted`): `probed`
  outgrowing `notified` under contention means the pub/sub path stopped
  delivering and the waiters live on the fallback probe.
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
at TTL. Pub/sub unreachable when a waiter subscribes → that wait polls
instead (warned once per occurrence); the command path is unaffected.

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
  redis:
    uri: redis://localhost:6379    # or host/port/username/password/database/ssl/timeout
  lock:
    type: redis
    key-prefix: "outbox:lock:"   # default
    wakeup: true                 # default: park waiters on release notifications (pub/sub)
```

That is all — with `event-outboxer.redis.uri` (or `.host`) set, the
starter creates and owns the Lettuce connection (ADR-0027), shares it
with [`event-outboxer-cache-redis`](event-outboxer-cache-redis.md),
and closes it on shutdown. With `lock.type: redis` it opens a second,
pub/sub connection on the same client for the wake-up
(`outboxRedisPubSubConnection`, closed first on shutdown); `lock.wakeup:
false` skips it and keeps the polling wait.

Alternatively, bring your own connection — any user-defined
`StatefulRedisConnection` bean wins and makes `event-outboxer.redis.*`
inert; with several connection beans, mark the outbox one with
`@OutboxRedisConnection`:

```java
@Bean(destroyMethod = "shutdown")
public RedisClient redisClient() {
    return RedisClient.create("redis://localhost:6379");
}

@Bean(destroyMethod = "close")
@OutboxRedisConnection   // needed only when several connection beans exist
public StatefulRedisConnection<String, String> redisConnection(RedisClient client) {
    return client.connect();
}
```

With `lock.type: redis` and neither properties nor a bean, startup
fails fast naming both remedies. When you bring your own connection,
add a `StatefulRedisPubSubConnection<String, String>` bean as well to
keep the wake-up (`client.connectPubSub()`); the locker takes the
`@OutboxRedisConnection`-qualified or the unique one, and polls with an
INFO line if there is none. A pub/sub bean is never picked as the
command connection.

The lock TTL passed to `tryLock` is the per-type
`event-outboxer.event-types.*.lock-ttl` (default 10 m).

### Without Spring

```java
EntityLocker locker = new RedisEntityLocker(connection);            // default prefix, polling wait
// or: new RedisEntityLocker(connection, "myapp:outbox:lock:")
// with the wake-up:
EntityLocker locker = RedisEntityLocker.builder()
        .connection(client.connect())
        .wakeupConnection(client.connectPubSub())
        .keyPrefix("myapp:outbox:lock:")        // optional
        .build();
new OutboxEngineBuilder().entityLocker(locker)/*...*/.build();
```

## Related

- [event-outboxer-lock-redisson](event-outboxer-lock-redisson.md) — the same guarantees on the application's Redisson client (Cluster/Sentinel, fair option); a different key type and prefix, never mixed with this one in a fleet.
- [event-outboxer-lock-postgres-lease](event-outboxer-lock-postgres-lease.md) — guarantee comparison across all lockers.
- ADRs: [0012](../adr/0012-extract-lock-key-on-handler.md), [0022](../adr/0022-lease-table-postgres-entity-locker.md) (§guarantee table), [0035](../adr/0035-bounded-lock-wait.md) (bounded wait and its wake-up).
