# event-outboxer-lock-redisson

Redis/KeyDB `EntityLocker` over a Redisson `RLock`
([ADR-0036](../adr/0036-redisson-entity-locker-module.md)): for
applications that already run Redisson and want the outbox's entity
locks on that client — its topologies (single, master-replica,
sentinel, cluster), its connection management, and its native pub/sub
wait for the bounded lock wait of ADR-0035.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-lock-redisson` |
| Java package | `io.github.bams22.outboxer.lock.redisson` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `org.redisson:redisson` (3.x) |
| Requires | Redis 7+ / KeyDB 6+, and a `RedissonClient` bean of the application's own — the starter never creates one |
| Enable with | `event-outboxer.lock.type: redisson` |

## What it does

**`RedissonEntityLocker`**:

- **Acquire** — `RLock.tryLock(0, ttl)`: one attempt, an explicit lease
  time. A `false` means busy → `Optional.empty()`.
- **Bounded wait (ADR-0035)** — `RLock.tryLock(lock-wait, ttl)`:
  Redisson subscribes to the lock's channel and parks until the
  holder's unlock publishes. No polling, no second connection to
  manage — the client's own pub/sub does it. `lock.wakeup: false`
  makes it poll through the SPI default instead.
- **Release** — `unlockAsync(ownerThreadId)`: the handle carries the
  acquiring thread's id, so it can be closed from any thread, and a
  release after the lease expired (or after another holder took the
  key) is reported by Redisson and logged at debug — a stale close can
  never free someone else's lock.
- **TTL always honoured, watchdog off** — every acquisition passes an
  explicit lease, which disables Redisson's lock watchdog. Without
  that, a stuck handler would have its lock renewed for as long as the
  JVM lives, the very failure mode ADR-0022 moved the PostgreSQL
  default away from. Exclusion therefore holds until
  `min(close(), ttl)`, a dead JVM's lock frees itself at TTL, and the
  engine's `lock-ttl ≥ handler-max-runtime` rule applies. No fencing
  at the business resource — same as the Lettuce locker.
- **No re-entrance** — an `RLock` is reentrant per (client, thread):
  the thread holding a key would get a second handle where the SPI
  promises "busy". The engine never re-enters (a handler thread holds
  one lock at a time), but the locker keeps the contract anyway with a
  per-thread guard, confirmed against Redis before it answers busy.
- **Fair option** — `lock.fair: true` uses `RFairLock`, which grants
  waiters in arrival order at the price of extra bookkeeping per
  acquisition. The outbox contract promises no per-key ordering, so
  the default is the plain `RLock`.
- Keys: `<key-prefix><key>`, default prefix **`outbox:rlock:`** —
  deliberately not the Lettuce locker's `outbox:lock:`. Redisson
  stores a hash where the Lettuce locker stores a string, so the two
  must never share a key, and **a fleet must not mix the two lockers**:
  their holders would not exclude each other. Redis Cluster users can
  put a hash tag in the prefix.

Failure semantics: Redis unreachable on acquire →
`LockAcquisitionException` → the dispatcher reschedules the event
after `lock-busy-retry-delay` *without consuming an attempt*;
unreachable on release → `LockReleaseException`, absorbed by the
engine (`onLockReleaseFailed`), key frees at TTL.

## When to use it

- Your application already has a `RedissonClient` (typically from
  `redisson-spring-boot-starter`) — the outbox then adds no second
  Redis client stack.
- Redis Cluster, Sentinel or master-replica: the Lettuce locker rides
  a single `StatefulRedisConnection`; Redisson brings the topology
  handling.
- You want waiters served in arrival order (`lock.fair`).

Prefer [lock-redis](event-outboxer-lock-redis.md) (Lettuce) when the
application has no Redisson: it is the lighter dependency, shares the
starter-managed connection with the metrics cache, and has its own
pub/sub wake-up. Prefer
[postgres-lease](event-outboxer-lock-postgres-lease.md) when
PostgreSQL is the only infrastructure.

## How to configure it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-lock-redisson</artifactId>
</dependency>
<!-- plus the application's Redisson, e.g. -->
<dependency>
    <groupId>org.redisson</groupId>
    <artifactId>redisson-spring-boot-starter</artifactId>
</dependency>
```

```yaml
event-outboxer:
  lock:
    type: redisson
    key-prefix: "outbox:rlock:"   # default for this locker
    fair: false                   # default; true = RFairLock
    wakeup: true                  # default; false = poll instead of Redisson's pub/sub wait
```

The client is resolved like every outbox dependency (ADR-0024,
ADR-0027): the `RedissonClient` bean marked with
`@OutboxRedissonClient` wins, else the unique or `@Primary` one, else
startup fails fast naming the candidates and the fix. The starter
never creates a `RedissonClient` — that is the point of this module —
and `event-outboxer.redis.*` (the starter-managed Lettuce connection)
is not involved.

```java
@Bean
@OutboxRedissonClient   // needed only when several RedissonClient beans exist
public RedissonClient outboxRedisson() {
    Config config = new Config();
    config.useSingleServer().setAddress("redis://localhost:6379");
    return Redisson.create(config);
}
```

The lock TTL passed to `tryLock` is the per-type
`event-outboxer.event-types.*.lock-ttl` (default 10 m); the wait is the
per-type `lock-wait` (default 100 ms).

### Without Spring

```java
EntityLocker locker = new RedissonEntityLocker(redissonClient);          // default prefix
// or:
EntityLocker locker = RedissonEntityLocker.builder()
        .client(redissonClient)
        .keyPrefix("myapp:outbox:rlock:")   // optional
        .fair(true)                         // optional
        .build();
new OutboxEngineBuilder().entityLocker(locker)/*...*/.build();
```

The locker does not own the client; shut it down yourself.

## Related

- [event-outboxer-lock-redis](event-outboxer-lock-redis.md) — the Lettuce locker; same guarantee level, different value type and prefix.
- [event-outboxer-lock-postgres-lease](event-outboxer-lock-postgres-lease.md) — guarantee comparison across all lockers.
- ADRs: [0036](../adr/0036-redisson-entity-locker-module.md), [0035](../adr/0035-bounded-lock-wait.md) (the bounded wait this locker serves natively), [0012](../adr/0012-extract-lock-key-on-handler.md), [0022](../adr/0022-lease-table-postgres-entity-locker.md) (§guarantee table).
