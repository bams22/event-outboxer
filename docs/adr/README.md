# Architecture Decision Records

Each ADR captures one significant architectural decision: what we decided,
why, which alternatives were considered and rejected. The format is based on
[Michael Nygard's ADR template](https://github.com/joelparkerhenderson/architecture-decision-record/blob/main/locales/en/templates/decision-record-template-by-michael-nygard/index.md).

## Statuses

- **Proposed** — under discussion.
- **Accepted** — decided and applied.
- **Superseded by ADR-XXXX** — replaced by a newer decision.
- **Deprecated** — obsolete but not yet replaced.

## Index

### Overall architecture and scope

- [ADR-0001: Local embedded outbox scope (not cross-service)](0001-local-embedded-outbox-scope.md)
- [ADR-0002: Publish participates in the caller's transaction](0002-participate-in-client-transaction.md)
- [ADR-0010: Storage-agnostic core via SPI](0010-storage-agnostic-core-via-spi.md)
- [ADR-0015: At-least-once semantics with idempotent handlers](0015-at-least-once-semantics.md)
- [ADR-0016: Maven module structure](0016-maven-module-structure.md)

### Payload and serialization

- [ADR-0003: Payload is an explicit DTO, not a lambda](0003-explicit-dto-payload.md)
- [ADR-0011: Jackson JSON as the only serializer in MVP](0011-jackson-json-only-in-mvp.md)
- [ADR-0012: extractLockKey() on the handler (not in PublishOptions, not in the DB)](0012-extract-lock-key-on-handler.md)

### Concurrency and isolation

- [ADR-0004: Per-event-type worker isolation](0004-per-event-type-worker-isolation.md)
- [ADR-0005: Worker heartbeat in a separate table](0005-workers-heartbeat-table.md)
- [ADR-0006: LISTEN/NOTIFY removed from MVP](0006-no-listen-notify-in-mvp.md)
- [ADR-0014: Optimistic locking via version](0014-optimistic-locking-via-version-field.md)

### Lifecycle and error handling

- [ADR-0007: FailureHandler chain-of-responsibility (replacing RetryPolicy)](0007-failure-handler-chain-of-responsibility.md)
- [ADR-0008: Three statuses + optional archive in a separate table](0008-three-statuses-plus-optional-archive.md)

### Spring integration

- [ADR-0009: Spring ThreadPoolTaskExecutor + TaskDecorator in the starter](0009-spring-task-executor-in-starter.md)
- [ADR-0013: OutboxListener as an event bus for observability](0013-outbox-listener-for-observability.md)
- [ADR-0017: Java 25 and Spring Boot 3.5.6 as the baseline](0017-java-25-and-spring-boot-3-5-baseline.md)

### Code conventions

- [ADR-0018: JSpecify for nullness annotations](0018-jspecify-for-nullness.md)

## Template

New ADRs are created from the [0000-template.md](0000-template.md).
