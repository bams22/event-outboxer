# ADR-0001: Local embedded outbox scope

## Status

Accepted — amended 2026-09-02 (the "user handler publishes to a broker"
pattern now ships packaged as the Spring Cloud Stream relay module; see
the Amendment section at the bottom)

## Date

2026-04-19

## Context

The Transactional Outbox is a popular pattern in microservice architectures.
It is tempting to extend it to a cross-service solution: one service writes
events to the outbox and another service reads from the same database and
processes them.

We need to explicitly define the scope of this library: does it work within a
single service, or can it serve as a bridge between microservices?

## Alternatives considered

- **A. Cross-service via shared DB**: the producer and the consumer connect
  to the same PostgreSQL instance and share the outbox table.
- **B. Cross-service via a broker**: the library is embedded into the
  producer, saves events to its own database, and the user writes an
  `EventHandler` that publishes to Kafka/RabbitMQ.
- **C. Local embedded only**: publisher and consumer are a single JVM
  process (or its replicas sharing their own database). Cross-service is
  out of scope.

## Decision

**Option C was chosen**: a local embedded outbox within a single service.

- `OutboxEventPublisher` and `OutboxEngine` live in a single JVM (or in
  replicas of a single service).
- The database is the service's own database; `event_outboxer.events` is a table
  owned by this service.
- Cross-service messaging is an orthogonal concern, solved by a user-provided
  `EventHandler` that publishes to a broker (Kafka, RabbitMQ).

## Rationale

**Shared DB between different services** (option A) is a classic anti-pattern:
- Tight coupling via the database schema: migrating `event_outboxer.events` requires
  coordination across every writer/reader.
- Broken data ownership: it is unclear who owns the data.
- The shared database becomes a scaling bottleneck.
- Both services have write access to the same table, enlarging the attack
  surface.

**Broker-based cross-service** (option B) is a workable architecture, but it
is **not the job of an outbox library**. Good tools already exist for that
(Debezium Outbox Router, Spring Cloud Stream). Our library provides local
atomicity and retry; broker integration is the responsibility of the user's
handler.

## Consequences

### For users

- The documentation contains an explicit section: "this library is a local
  embedded outbox, not cross-service messaging".
- For cross-service flows, a broker plus an `EventHandler` that publishes to
  it are required. An example is shown in
  [docs/ARCHITECTURE.md](../ARCHITECTURE.md).

### For maintainers

- We do NOT introduce producer-only / consumer-only modules that assume a
  shared database between services.
- Documentation and examples avoid cross-service scenarios.
- If a request for cross-service features arrives, we direct users to the
  broker-based approach.

### Positive consequences

- Smaller scope → simpler API.
- Clear responsibility: local atomicity + retry; everything else is user code.
- We avoid bad practices (shared DB).

### Negative consequences

- Users hoping for a "simple cross-service outbox" will discover that a
  broker is required. Requires explicit documentation that explains the
  correct path.

## Amendment (2026-09-02): the broker-publishing handler ships as a module

ADR-0032 adds `event-outboxer-relay-spring-cloud-stream` — a facade
plus a built-in `EventHandler` that delivers stored messages to a
broker through Spring Cloud Stream's `StreamBridge`. This does NOT
change the scope decided here: the outbox stays embedded and
per-service, no service reads another service's tables, and delivery
still goes through a broker exactly as option B describes. The module
merely packages the handler that this ADR always expected users to
write themselves.

## Related decisions

- [ADR-0002](0002-participate-in-client-transaction.md) — atomicity within
  the caller's transaction (in a single service).
- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — adapters are isolated
  from the engine, which makes adding non-SQL backends easier if needed.
- [ADR-0032](0032-spring-cloud-stream-relay-module.md) — the packaged
  broker-publishing handler (see the Amendment above).
