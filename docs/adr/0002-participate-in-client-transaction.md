# ADR-0002: Publish participates in the caller's transaction

## Status

Accepted

## Date

2026-04-19

## Context

The core value of the Transactional Outbox is atomicity between a business
operation and the persistence of an event. If `@Transactional void
createOrder()` rolls back, the event MUST NOT be persisted (and therefore
MUST NOT be processed).

This requires `EventStore.save()` to run on **the same JDBC connection** that
Spring's `DataSourceTransactionManager` bound to the current transaction.
Otherwise the event is written in a separate transaction, independent of the
business transaction.

While studying jobrunr, we discovered that in its default configuration it
does NOT provide this guarantee — `DefaultSqlStorageProvider` calls
`dataSource.getConnection()` directly and obtains a fresh connection from the
pool, separate from Spring's. Business TX and event write end up in different
transactions — a classic race condition.

## Alternatives considered

- **A. Explicit `TransactionAwareDataSourceProxy`**: the starter automatically
  wraps the DataSource before passing it to the EventStore adapter. The
  adapter simply calls `dataSource.getConnection()` — the proxy does the
  magic.
- **B. `DataSourceUtils.getConnection()` in the adapter**: the adapter
  explicitly uses Spring's API to obtain a connection that respects the
  current TX.
- **C. Explicit `TransactionContext` in core + adapter**: similar to
  db-scheduler's `JdbcRunner` — an abstraction over TX that can be swapped
  out.
- **D. Require the user to wrap the DataSource themselves**: jobrunr's
  approach.

## Decision

**Option A was chosen** for the runtime integration + **C** for the
architecture.

- **In the core**: `EventStore.save(PendingEvent)` guarantees participation
  in the current TX; the adapter decides how that is achieved.
- **In the PG adapter** (`storage-postgres`): uses the standard Spring
  pattern `DataSourceUtils.getConnection(dataSource)`. The adapter itself
  does not contain Spring-specific code — a `ConnectionSupplier` SPI is
  used, which in `spring-boot-starter` is configured via `DataSourceUtils`.
- **In the starter**: the `DataSource` bean is automatically wrapped in a
  `TransactionAwareDataSourceProxy`, mirroring
  [db-scheduler](https://github.com/kagkarlsson/db-scheduler)'s approach in
  `DbSchedulerConfigurationSupport:134-144`. If the user has already wrapped
  it, we do not wrap twice.

## Rationale

- `TransactionAwareDataSourceProxy` is a proven Spring mechanism. It
  neutralizes `commit()`/`rollback()` on connections participating in an
  outer TX and delegates to `DataSourceUtils.getConnection()` under the
  hood. db-scheduler uses this exact pattern, and it has worked reliably in
  production for years.
- Automatic wrapping in the starter means users do not need to know the
  details. "Works out of the box" is a key UX property.
- The core remains Spring-agnostic: the `EventStore.save()` port takes a
  `PendingEvent` and knows nothing of Spring; the adapter uses a simple
  `ConnectionSupplier`, which can be a plain `ds::getConnection` for
  plain-Java usage, or a Spring-aware variant for Spring Boot.

## Consequences

### For users

- `@Transactional void createOrder()` → `publisher.publish(...)` →
  `orderRepo.save(...)` → commit/rollback of the business TX determines
  whether the event ends up in the event-outboxer.
- Hard contract documented: **publish() MUST participate in the current TX**.
- Behavior when there is no TX is configurable:
  `event-outboxer.publisher.no-transaction-policy: FAIL | IGNORE`.

### For maintainers

- The starter ALWAYS wraps the DataSource in
  `TransactionAwareDataSourceProxy` during autoconfiguration. If the user
  wants the DataSource unwrapped, they must opt out explicitly.
- The PG adapter NEVER calls `dataSource.getConnection()` directly — it uses
  the `ConnectionSupplier` port, which lets the starter inject the correct
  behavior.

### Positive consequences

- Atomicity out of the box. Users cannot accidentally break it.
- A regression test in CI: `@Transactional` + `publish()` + synthetic
  rollback → no entry in `event_outboxer.events`.

### Negative consequences

- Hidden dependency on a Spring pattern. Plain-Java users (without Spring)
  must manage connection lifecycle themselves through `ConnectionSupplier`.

## Related decisions

- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — the core knows nothing
  about Spring; all Spring integration lives in the starter.
- [ADR-0009](0009-spring-task-executor-in-starter.md) — same pattern:
  Spring-specific `ThreadPoolTaskExecutor` lives only in the starter.
- [ADR-0024](0024-outbox-datasource-selection.md) — which `DataSource` the
  starter wraps when the application defines several
  (`@OutboxDataSource` qualifier → primary/unique → fail fast).
