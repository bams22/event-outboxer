# ADR-0020: In-memory storage is test infrastructure only

## Status

Accepted

## Date

2026-07-26

## Context

The starter's in-memory storage auto-configuration had
`matchIfMissing = true`: an application with a PostgreSQL `DataSource`
that forgot the single line `event-outboxer.storage.type: postgres`
silently ran the outbox on a `ConcurrentHashMap`. The failure mode was
the worst kind — invisible: the application started, `publish()`
"worked", the `FAIL` no-transaction policy was satisfied, yet a
transaction rollback did not roll the event back (the library's core
atomicity promise was gone) and a restart lost everything.

The deeper observation (project owner): **a transactional outbox
without a durable store is pointless — one might as well submit tasks
straight to an executor.** A non-durable mode therefore does not
deserve to be a configuration option at all, let alone a default.

Why the `event-outboxer-storage-inmemory` module cannot simply be
deleted or folded into the testkit:

- core's unit tests run against it (fast, no Testcontainers —
  ADR-0016's testing model);
- it is the reference implementation for the SPI contract tests;
- the testkit's plain-Java `OutboxTestContext` builds on it;
- moving its classes into the testkit would create a module cycle
  (core(test) → testkit → core).

## Decision

1. **In-memory storage is unreachable through configuration.**
   `StorageType.inmemory` and `InMemoryStorageAutoConfiguration` are
   removed from the starter. `event-outboxer.storage.type` has **no
   default**; an outbox without configured storage fails at startup.
2. **The failure is actionable.** `OutboxStorageFailureAnalyzer` turns
   the raw `NoSuchBeanDefinitionException: EventStore` into a
   diagnosis: type unset → set `postgres` / use the test import;
   `postgres` without a `DataSource` → add one; `postgres` without the
   adapter module → add the dependency.
3. **Tests opt in explicitly.**
   `OutboxInMemoryTestConfiguration` (starter, plain
   `@Configuration`, never auto-registered) wires the in-memory
   store/registry/admin for Spring tests:
   `@SpringBootTest @Import(OutboxInMemoryTestConfiguration.class)`
   plus `event-outboxer-storage-inmemory` in test scope.
4. **The module stays, re-described as test infrastructure** — its
   published artifact continues to serve the testkit, contract tests
   and user test suites, but no production configuration path leads
   to it.

## Consequences

- **Breaking vs 0.2.x.** Applications relying on the implicit (or
  explicit) `inmemory` storage type fail at startup after upgrading.
  Migration: production — set `event-outboxer.storage.type=postgres`;
  tests — replace the property with the explicit `@Import`.
- A misconfigured production deployment now fails loudly at startup
  instead of silently losing events — the failure the library exists
  to prevent.
- The starter has one less auto-configuration and one less silent
  default; db-scheduler / jobrunr behave the same way (no store — no
  start).
- Plain-Java users of `OutboxEngineBuilder` are unaffected: the
  builder accepts any `EventStore`, and wiring `InMemoryEventStore`
  there remains a deliberate code-level act.

## Related decisions

- [ADR-0001](0001-local-embedded-outbox-scope.md) — durable per-service
  outbox is the product.
- [ADR-0002](0002-participate-in-client-transaction.md) — the atomicity
  promise the silent in-memory default betrayed.
- [ADR-0016](0016-maven-module-structure.md) — module layout; the
  in-memory module's role narrows to test infrastructure.
