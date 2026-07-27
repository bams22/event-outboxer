# ADR-0024: Outbox DataSource selection via the @OutboxDataSource qualifier

## Status

Accepted.

## Date

2026-07-28

## Context

Every outbox JDBC consumer in the starter — the PostgreSQL storage
adapter's `ConnectionSupplier`, both PostgreSQL entity lockers and the
lease-table probe — injected the application's `DataSource` as a plain
by-type parameter. In an application with a single `DataSource` that is
exactly right; in an application with several (orders DB + reporting
DB, tenant shards, a read replica) it breaks down:

- with no `@Primary` bean the context fails with Spring's stock
  `NoUniqueBeanDefinitionException` — no hint that the outbox is the
  unsatisfied consumer, no way to point it at the right database;
- with a `@Primary` bean the outbox silently follows it, even when the
  primary is *not* the database that holds the outbox tables.

The user needs a way to say "the outbox lives in *this* database".

## Alternatives considered

- **A. Marker qualifier annotation** — a no-member meta-annotation over
  Spring's `@Qualifier`, the exact pattern Spring Boot itself ships as
  `@FlywayDataSource`, `@BatchDataSource` and `@QuartzDataSource` for
  the same "subsystem needs its own DataSource" problem.
- **B. Bean-name property** (`event-outboxer.storage.datasource-bean-name`)
  resolved via `BeanFactory.getBean(name, DataSource.class)`. Stringly
  typed, invisible to IDE navigation and refactoring, and a second
  configuration surface for the same fact.
- **C. Eager-fallback qualifier** — qualified `ObjectProvider` with a
  *plain* `DataSource` parameter as the `getIfAvailable` fallback (the
  pattern in an internal reference implementation). Broken by
  construction: the plain fallback parameter is injected eagerly, so
  with several unmarked beans the context dies on the fallback before
  the qualified bean is even consulted — the qualifier only works if
  the same bean is *also* `@Primary`, defeating its purpose.
- **D. Per-subsystem qualifiers** — separate qualifiers for storage and
  for the lockers, as the reference implementation does for its Flyway
  DataSource.

## Decision

**Option A** — a single `@OutboxDataSource` marker qualifier in
`io.github.bams22.outboxer.spring`, resolved by a shared
`OutboxDataSourceResolver` used by every outbox JDBC consumer:

1. the single bean marked `@OutboxDataSource` — wins even over an
   unrelated `@Primary` bean (qualifier filtering precedes the primary
   tie-break);
2. otherwise the unique `DataSource` bean, or the `@Primary` one among
   several (`ObjectProvider.getIfUnique()`);
3. otherwise startup fails fast with
   `AmbiguousOutboxDataSourceException` naming the candidate beans and
   the fix, rendered by a dedicated `FailureAnalyzer`. Two beans both
   carrying the qualifier fail the same way — exactly one may.

Both lookups go through `ObjectProvider`, never an eagerly injected
parameter — this is what makes rule 1 actually reachable when rule 3
would otherwise apply (the flaw of alternative C).

Supporting decisions:

- **One qualifier for all outbox JDBC** (rejecting D): the outbox
  tables, the lease table and advisory locks live in the same database
  by design — a per-subsystem split would let them drift apart.
  Exotic setups still have the bean-level escape hatch: user-defined
  `ConnectionSupplier` / `EntityLocker` beans override the wiring
  entirely (`@ConditionalOnMissingBean`).
- **Lockers unwrap `TransactionAwareDataSourceProxy`**: if the
  qualified bean is (or is wrapped in) the transaction-aware proxy,
  the lockers strip it back to the raw target — their acquire/release
  must run as their own autocommit statements, never bound to the
  caller's transaction (ADR-0022 §JDBC contract). Only this proxy type
  is unwrapped; other `DelegatingDataSource` wrappers pass through
  deliberately. The storage adapter keeps wrapping the resolved
  DataSource per ADR-0002.
- **Diagnostics resolve leniently**: the HikariCP pool-exhaustion
  warning (ADR-0012) uses a non-throwing variant — ambiguity skips the
  warning, it never fails startup.
- **No property-based selection** (rejecting B) in this iteration; it
  can be added later without breaking the qualifier path.

## Rationale

- Matches the platform convention users already know from Spring Boot
  (`@FlywayDataSource` et al.) — zero new concepts.
- Type-safe and discoverable: IDE find-usages on the annotation shows
  exactly where the outbox DataSource comes from.
- Fail-fast beats fail-silent: with several unmarked candidates the
  previous behaviour was an opaque `NoUniqueBeanDefinitionException`;
  now the error names the beans and both fixes (`@OutboxDataSource` or
  `@Primary`).

## Consequences

### For users

- Single-DataSource applications: nothing changes, nothing to do.
- Multi-DataSource applications: mark the outbox database's bean with
  `@OutboxDataSource` (or keep relying on `@Primary`). A `DataSource`
  defined purely through `spring.datasource.*` properties cannot carry
  the annotation and participates via the primary/unique rule.

### For maintainers

- Every new starter-side `DataSource` consumer must go through
  `OutboxDataSourceResolver` — never a plain by-type parameter, which
  would silently reintroduce alternative C's eager-injection failure.

## Related decisions

- [ADR-0002](0002-participate-in-client-transaction.md) — the resolved
  DataSource is still wrapped in `TransactionAwareDataSourceProxy` by
  the storage auto-configuration.
- [ADR-0022](0022-lease-table-postgres-entity-locker.md) — the raw
  autocommit JDBC contract that motivates the proxy unwrap for
  lockers.
