# ADR-0036: Redisson entity locker module

## Status

Accepted — 2026-09-05. Measured on the benchmark harness the same day
(see §Measured).

## Date

2026-09-05

## Context

The Redis-backed `EntityLocker` (`event-outboxer-lock-redis`) rides
Lettuce: `SET NX PX` with a fencing token, a Lua compare-and-delete
release, and — since ADR-0035 — a pub/sub wake-up for the bounded lock
wait on a second, starter-managed connection (ADR-0027). It is the
right shape for an application whose only Redis client is the
starter's.

Many Spring applications already run Redisson, usually through
`redisson-spring-boot-starter`, for caches, rate limiters or their own
distributed locks. For them the Lettuce locker is the *second* Redis
client stack, with its own connection to configure and monitor — the
mirror image of the argument ADR-0035 made when it rejected Redisson
as the mechanism for the bounded wait. Redisson's `RLock` also brings
what the Lettuce locker does not have: Cluster, Sentinel and
master-replica topologies out of the box, a pub/sub wait built into
`tryLock(waitTime, leaseTime)`, and a fair variant.

The question is not whether to replace the Lettuce locker — ADR-0035's
reasoning against Redisson as a dependency of the *default* path
stands — but whether a separate, opt-in module for Redisson shops is
worth its surface, and how to build it so that Redisson's semantics
cannot undermine the engine's.

## Alternatives considered

- **No module; document how to wrap `RLock` in a user-defined
  `EntityLocker` bean.** Zero surface, but every team would rediscover
  the same three traps (watchdog, ownership, re-entrance) and get at
  least one wrong. Rejected.
- **Redisson inside `event-outboxer-lock-redis` as an optional code
  path.** Two client stacks in one module, two sets of optional
  dependencies, and `@ConditionalOnClass` gymnastics in the starter.
  Rejected; ADR-0016's one-module-per-target rule applies.
- **A starter-managed `RedissonClient` from `event-outboxer.redis.*`.**
  Would make the module usable without Redisson in the application —
  but that is exactly the case the Lettuce locker already serves, and
  a starter-owned Redisson client would reintroduce the second stack
  the module exists to avoid. Rejected: the module rides the
  application's client or fails fast.

## Decision

1. **A separate module `event-outboxer-lock-redisson`** (package
   `io.github.bams22.outboxer.lock.redisson`), depending on `-api`,
   `-spi` and `org.redisson:redisson` — never on `-core`. `RedissonEntityLocker`
   implements `EntityLocker` over `RedissonClient.getLock(name)` (or
   `getFairLock`).
2. **The three Redisson traps are closed in the adapter, not in
   documentation:**
   - *Watchdog off.* Every acquisition passes an explicit lease time
     (`ttl`). Without one Redisson's watchdog renews the lock for as
     long as the JVM lives, so a stuck handler would hold its key
     forever — the advisory-locker failure mode ADR-0022 moved away
     from, and a violation of the `lockTtl >= handlerMaxRuntime`
     contract of ADR-0012's amendment. With the lease the guarantee is
     the Lettuce locker's: exclusion until `min(close(), ttl)`, crash
     release at TTL, no fencing at the resource.
   - *Release from any thread; stale release a no-op.* Redisson
     ownership is per (client, thread), so a plain `unlock()` from
     another thread fails. The handle carries the acquiring thread's
     id and releases with `unlockAsync(threadId)`; a release after
     expiry or takeover is reported by Redisson's script and logged at
     debug — the token-checked release of the Lettuce locker, in
     Redisson terms.
   - *No re-entrance.* `RLock` is reentrant per (client, thread); the
     SPI contract says a held key is busy, for every caller. The
     engine never re-enters (a handler thread holds one lock at a
     time), but the adapter keeps the contract with a per-thread
     guard, confirmed against Redis before answering busy so that an
     unnoticed expiry does not shadow a key.
3. **The bounded wait is Redisson's own.** `tryLock(key, ttl, maxWait)`
   maps to `RLock.tryLock(maxWait, ttl)`: Redisson subscribes to the
   lock's channel and parks until the holder's unlock publishes. No
   `LockWaiters`, no second connection. `lock.wakeup: false` falls
   back to the SPI's polling default.
4. **Keys are kept apart from the Lettuce locker's.** Default prefix
   `outbox:rlock:` against `outbox:lock:`: Redisson stores a hash where
   Lettuce stores a string, so a shared key would fail with
   `WRONGTYPE`, and holders of the two lockers do not exclude each
   other in any case — a fleet must not mix them. `lock.key-prefix`
   became unset-by-default in the starter so each locker keeps its own
   default.
5. **Starter integration mirrors ADR-0024 and ADR-0027:**
   `event-outboxer.lock.type: redisson`, a `@OutboxRedissonClient`
   qualifier, resolution qualified → unique/`@Primary` → fail fast
   naming the candidates. The starter never creates a `RedissonClient`.
   `lock.fair` (default `false`) selects `RFairLock`.
6. **The harness grows `--bench.lock=redisson`**, defining the
   `RedissonClient` in the worker context the way an application
   would.

## Rationale

The module's value is conditional on the application already having
Redisson, and then it is real: one Redis client stack instead of two,
topology support the Lettuce locker would need its own code for, and
a wait that needs no extra connection. Its cost is bounded by being
separate — nothing changes for anyone who does not add it — and by
closing the three semantic traps in code rather than trusting every
integrator to know them.

Fairness is offered but not promised: the outbox contract has no
per-key ordering (ADR-0035 §3), and `RFairLock` costs extra
bookkeeping per acquisition.

## Measured

Same laptop, standalone Redis 7 and PostgreSQL 15 containers as the
[lock-wait session](../benchmarks/2026-09-04-laptop-lock-wait.md);
one run per cell; recorded in that session's
[Redisson addendum](../benchmarks/2026-09-04-laptop-lock-wait.md#addendum-2026-09-05-redisson-locker).
On the `hot-key` preset with the 100 ms wait both lockers sit on the
engine's 3.00-row floor with the same drain rate and tail within noise
(Redisson 513/s, Lettuce 574/s; max e2e 3.36 s both); on unique keys
1 312 vs 1 567/s. Redisson spends 2.4× the Redis commands per event
(12 vs 5 on unique keys, 17 vs 7 under contention): an `RLock` acquire
and release are multi-call Lua scripts. `fair` adds another 12
commands per event and tightens the tail (p95 2.8 s vs 3.3 s). On the
uncapped virtual executor Redisson's in-client wait fares a little
better (410 vs 326/s). Conclusion: Redisson's native wait and the
Lettuce locker's pub/sub wake-up are the same mechanism and land in
the same place — the module is chosen for the client the application
already has and for topology, not for speed, and it costs the Redis
more per event.

## Consequences

**Users.** A fourth locker: `lock.type: redisson` for Redisson shops
and clustered Redis. Guarantees identical to the Lettuce locker's.
Two Redis lockers must never share a fleet or a key prefix.
`lock.key-prefix` no longer defaults to `outbox:lock:` in the
properties object — it is unset, and each locker applies its own
default; an explicit value behaves as before.

**Maintainers.** One more module to release, with Redisson's version
managed in the parent (not the Spring Boot BOM). The re-entrance guard
and the thread-id release are load-bearing: the SPI contract tests
run against this adapter and would catch either being dropped. Redis
Cluster is supported by construction but not exercised by the test
suite (single-node Testcontainers).

**Operations.** Redisson's own connection pool and Netty threads carry
the lock traffic; nothing of the outbox's `event-outboxer.redis.*`
applies. The lock keys are hashes under `outbox:rlock:`, visible with
`SCAN`; a stuck lock frees itself at `lock-ttl`.

## Related decisions

- [ADR-0035](0035-bounded-lock-wait.md) — the bounded wait this
  locker serves through `RLock.tryLock(waitTime, leaseTime)`; its
  Alternatives section rejected Redisson as the mechanism for the
  default path, which this ADR does not revisit.
- [ADR-0027](0027-starter-managed-redis-connection.md) — the Lettuce
  connection and the resolution pattern this module mirrors.
- [ADR-0022](0022-lease-table-postgres-entity-locker.md) — the
  guarantee table; Redisson sits on the Redis row.
- [ADR-0016](0016-maven-module-structure.md) — one module per target
  dependency.
