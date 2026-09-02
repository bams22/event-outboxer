# ADR-0027: Starter-managed Redis connection and the @OutboxRedisConnection qualifier

## Status

Accepted.

## Date

2026-08-16

## Context

Two starter features run over Redis/KeyDB via Lettuce: the entity
locker (`event-outboxer.lock.type=redis`, `event-outboxer-lock-redis`)
and the shared metrics-snapshot cache
(`event-outboxer.cache.type=redis`, `event-outboxer-cache-redis`).
Both auto-configurations were gated on
`@ConditionalOnBean(StatefulRedisConnection.class)` — the starter never
created a connection, so every user had to hand-write the same two
beans (`RedisClient` + `StatefulRedisConnection<String, String>`) and
manage their lifecycle. That contradicted the starter's own bar for the
PostgreSQL path, where `spring.datasource.*` alone yields a working
outbox.

The `@ConditionalOnBean` gate also produced a poor failure mode: with
`lock.type=redis` and no connection bean, the lock auto-configuration
silently backed off, no `EntityLocker` existed, and the context died
with a raw `NoSuchBeanDefinitionException: EntityLocker` — no hint
that a missing Redis connection was the cause.

Finally, applications with several Redis connections (sessions cache +
business data, per-tenant instances) had no way to say "the outbox
uses *this* one" — the same gap ADR-0024 closed for `DataSource`.

## Alternatives considered

- **A. Keep bring-your-own-bean only** — smallest surface, but the
  boilerplate and the undiagnosed failure remain; rejected.
- **B. Reuse Spring Data Redis (`spring.data.redis.*`)** — would drag
  `spring-boot-starter-data-redis` and its `RedisConnectionFactory`
  abstraction into a library that talks plain Lettuce; the adapters
  take a raw `StatefulRedisConnection`, so the starter would have to
  unwrap the factory anyway. Rejected.
- **C. Starter-owned connection from `event-outboxer.redis.*` plus a
  qualifier mirroring ADR-0024** — chosen.

## Decision

1. **New shared property group `event-outboxer.redis.*`** — `uri`
   (full `RedisURI`, wins when set; covers sentinel and most TLS
   cases) or discrete `host`/`port`/`username`/`password`/`database`/
   `ssl`/`timeout`/`client-name`. One group, because the locker and
   the cache share a single connection by design.
2. **`RedisConnectionAutoConfiguration`** — when Lettuce is on the
   classpath, `uri` or `host` is set, and the application defines no
   `StatefulRedisConnection` bean of its own, the starter creates the
   client and connection. A single package-private lifecycle owner
   (`OutboxLettuceConnectionManager`, a `DisposableBean`) holds both
   and destroys them in order (connection `close()`, then client
   `shutdown()`); the exposed connection bean (name
   `outboxRedisConnection`, part of the documented contract) uses
   `@Bean(destroyMethod = "")` so Spring never infers a second
   `close()`. No public `RedisClient` bean is registered — it would
   break user injection points expecting a unique one. The connection
   is opened eagerly, so a down Redis fails startup with an actionable
   message; `timeout` bounds the wait.
3. **`@OutboxRedisConnection` qualifier + resolver**, mirroring
   ADR-0024 exactly: the qualified bean wins (even over `@Primary`),
   else the unique/`@Primary` bean, else startup fails fast via
   `AmbiguousOutboxRedisConnectionException` +
   `OutboxRedisConnectionFailureAnalyzer` naming the candidates and
   the fix. The starter-created connection carries the qualifier
   itself. Both Redis consumers resolve through
   `OutboxRedisConnectionResolver` with the lazy `ObjectProvider`
   triple.
4. **Fail-fast replaces silent back-off.** The
   `@ConditionalOnBean(StatefulRedisConnection.class)` gates are
   removed from `RedisLockAutoConfiguration` and
   `RedisCacheAutoConfiguration`: `lock.type=redis` /
   `cache.type=redis` are explicit opt-ins, so a missing connection is
   a configuration error diagnosed at startup ("set
   `event-outboxer.redis.uri` or define a connection bean"), not a
   condition to skip. This converts the former cryptic
   missing-`EntityLocker` failure into a diagnosed one — and turns the
   one case that previously *booted* (redis lock type with no
   connection and no storage configured) into a startup failure.
5. **Precedence**: user connection bean > `event-outboxer.redis.*`
   properties (which become inert); within properties `uri` > discrete
   fields (mirrors `spring.data.redis.url`); user-defined
   `EntityLocker` / `MetricsSnapshotCache` beans still displace the
   respective consumer entirely, skipping connection resolution.

## Rationale

- Property-driven wiring brings the Redis path to parity with the
  PostgreSQL path: one YAML block, no hand-written beans, correct
  shutdown ordering for free.
- Mirroring ADR-0024 (same resolution order, same failure-analyzer
  pattern, same "one qualifier governs all outbox uses" rule) keeps
  the mental model uniform: `@OutboxDataSource` for JDBC,
  `@OutboxRedisConnection` for Redis.
- A single lifecycle owner avoids the classic double-close/leak bugs
  of separate client and connection beans with inferred destroy
  methods.
- Fail-fast follows the project's ADR-0024 stance: no silent
  fallbacks on explicit opt-ins.

## Consequences

### For users

- `event-outboxer.redis.uri: redis://…` (or `.host`) is now all that
  is needed for `lock.type=redis` / `cache.type=redis`.
- Existing bring-your-own-bean setups keep working unchanged; the
  qualifier is only needed when several connection beans exist.
- **Behavior change**: `lock.type=redis` or `cache.type=redis` with no
  resolvable connection now fails startup with a diagnosis (previously
  a cryptic `NoSuchBeanDefinitionException`, or a silent boot when the
  engine itself backed off).
- Redis Cluster and custom `ClientResources` are deliberately out of
  scope for the managed connection — use the bean path
  (`RedisClusterClient` connections are a different Lettuce type
  anyway).

### For maintainers

- The resolver/exception/analyzer trio in
  `io.github.bams22.outboxer.spring.redis` mirrors the ADR-0024
  machinery; changes to one should be reflected in the other.
- `RedisLockAutoConfiguration` / `RedisCacheAutoConfiguration` are
  ordered `after = RedisConnectionAutoConfiguration.class`; keep that
  ordering if any bean-presence gate is ever reintroduced.

## Related decisions

- [ADR-0024](0024-outbox-datasource-selection.md) — the
  `@OutboxDataSource` pattern this ADR mirrors.
- [ADR-0022](0022-lease-table-postgres-entity-locker.md) — locker
  guarantee table; the Redis locker's per-call-UUID discipline.
- [ADR-0012](0012-extract-lock-key-on-handler.md) — per-backend lock
  guarantee amendment.
