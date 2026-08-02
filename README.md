# event-outboxer

[![CI](https://github.com/bams22/event-outboxer/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/bams22/event-outboxer/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Embedded transactional outbox for Java 25 / Spring Boot 3.5.6 / PostgreSQL 15.

A library for asynchronous event-driven processing with atomicity guarantees
between business transactions and event persistence. Inspired by
[db-scheduler](https://github.com/kagkarlsson/db-scheduler) and
[jobrunr](https://github.com/jobrunr/jobrunr), but purpose-built for the
Transactional Outbox pattern.

## Key properties

- **Atomicity**: `publish()` participates in the caller's current transaction
  via `TransactionAwareDataSourceProxy`. Business data and events commit or
  roll back together — no inconsistency windows.
- **Storage-agnostic core**: the engine knows nothing about the database;
  the `EventStore` port is implemented in separate modules (PostgreSQL in
  MVP, other backends are pluggable).
- **Per-event-type isolation**: each `EventHandler` gets its own
  `ThreadPoolTaskExecutor` and poller — a slow type cannot block a fast one.
- **Distributed-safe**: `SELECT FOR UPDATE SKIP LOCKED` + optimistic locking
  via a `version` column + heartbeat/lease in a separate `event_outboxer.workers`
  table for detecting crashed workers.
- **At-least-once**: handlers must be idempotent. Exponential backoff with
  jitter, attempt limits, DISABLED status for poison events.
- **Composable failure handling**: `FailureHandler<T>` chain (log →
  max-retries → backoff → listener-notify) — each handler type may have its
  own policy.
- **End-to-end trace continuity**: the trace active at `publish()` continues
  into handler execution — a PRODUCER span per insert, its W3C context stored
  with the event, a CONSUMER span per handler attempt. Optional adapters for
  OpenTelemetry (`event-outboxer-tracing-otel`, works with the OTel Java
  agent) and Micrometer Tracing (`event-outboxer-tracing-micrometer`),
  auto-detected by the starter.
- **Deep Spring Boot integration**: MDC / tracing / security-context
  propagation via `ContextPropagatingTaskDecorator`, graceful shutdown
  through `SmartLifecycle`.

## Scope

**Fits well for**: atomicity within a single service (or its replicas) —
when you need to save business data and schedule background work atomically.
Examples: "save the order and send the confirmation email", "update the user
and invalidate the cache", splitting heavy calculations into background
steps.

**Not suitable for**: cross-service messaging. If an event must reach another
service, use a broker (Kafka, RabbitMQ); the library provides an integration
point via a handler that publishes to the broker. Shared DB between services
is an anti-pattern and is explicitly not supported.

## Quick start

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.bams22</groupId>
            <artifactId>event-outboxer-bom</artifactId>
            <version>${outboxer.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.github.bams22</groupId>
        <artifactId>event-outboxer-spring-boot-starter</artifactId>
    </dependency>
    <dependency>
        <groupId>io.github.bams22</groupId>
        <artifactId>event-outboxer-storage-postgres</artifactId>
    </dependency>
    <dependency>
        <groupId>io.github.bams22</groupId>
        <artifactId>event-outboxer-lock-postgres-lease</artifactId>
    </dependency>
</dependencies>
```

```yaml
# application.yml
spring.flyway.locations:
  - classpath:db/migration
  - classpath:db/migration/outbox/core
  - classpath:db/migration/outbox/lock   # only with event-outboxer.lock.type=postgres-lease
```

```java
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final OutboxEventPublisher publisher;

    @Transactional
    public void createOrder(CreateOrderCommand cmd) {
        Order order = orderRepository.save(Order.from(cmd));
        publisher.publish(
            "SEND_ORDER_CONFIRMATION",
            new SendOrderConfirmationPayload(order.id(), order.email())
        );
        // if the transaction rolls back, the event is NOT persisted
    }
}

@Component
@RequiredArgsConstructor
public class SendOrderConfirmationHandler
        implements EventHandler<SendOrderConfirmationPayload> {

    private final EmailService emailService;

    @Override public String eventType() { return "SEND_ORDER_CONFIRMATION"; }
    @Override public Class<SendOrderConfirmationPayload> payloadType() {
        return SendOrderConfirmationPayload.class;
    }

    @Override
    public EventOutcome handle(EventContext ctx, SendOrderConfirmationPayload p) {
        emailService.send(p.email(), "Order " + p.orderId() + " confirmed");
        return EventOutcome.Success.INSTANCE;
    }
}
```

## Observability at a glance

When Spring Boot Actuator is on the classpath, the starter auto-wires
a health endpoint at `GET /actuator/health/outbox` (engine state +
backlog totals + worker id) and — when a `MeterRegistry` bean is
present — a full set of Micrometer counters / timers / summaries
prefixed `event_outboxer.*` (per-`event_type` tags on every signal),
plus an `event_outboxer.engine.state{state=...}` gauge for
metric-based alerting. For k8s deployments that probe only
`/actuator/health/liveness` and `/actuator/health/readiness`, opt in
to probe integration with `event-outboxer.health.probe-groups: [readiness]`
so rolling restarts drain in-flight handlers automatically. Both the
DB schema (`event_outboxer` by default) and the metric prefix are
configurable via `event-outboxer.storage.schema` and `event-outboxer.metrics.prefix`
so the library never clashes with siblings in the same deployment.
See [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) for the field-level
health reference, the metrics catalogue, the k8s probe playbook and
five troubleshooting recipes.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — overview, module layout,
  key components and data flows.
- [Module reference](docs/modules/README.md) — one page per Maven
  module: purpose, when to use it, how to configure it.
- [Configuration](docs/CONFIGURATION.md) — full `application.yml`
  reference.
- [Storage: PostgreSQL](docs/STORAGE.md) — database schema, key queries,
  migrations.
- [Observability](docs/OBSERVABILITY.md) — health indicator, Micrometer
  metrics table, `OutboxListener` callback catalogue, troubleshooting
  playbook.
- [Testing](docs/TESTING.md) — using `event-outboxer-testkit` to write
  deterministic handler tests without Testcontainers.
- [Glossary](docs/GLOSSARY.md) — definitions (event, claim, lease,
  orphan, etc.).
- [Architecture Decision Records](docs/adr/README.md) — rationale for
  every significant design decision.
- [Artifacts](ARTIFACTS.md) — module matrix: which jars to add for
  each use case, compatibility, coordinates.
- [Changelog](CHANGELOG.md) — release notes.

## Example

A runnable Spring Boot 3 + PostgreSQL example lives at
[`examples/spring-boot-postgres/`](examples/spring-boot-postgres/). One
`docker compose up` and `mvn spring-boot:run` is enough — see its
[README](examples/spring-boot-postgres/README.md) for the full walkthrough.

## References and inspiration

- [db-scheduler](https://github.com/kagkarlsson/db-scheduler) — minimalist
  job scheduler (single-table design, polished polling, excellent test
  helpers).
- [jobrunr](https://github.com/jobrunr/jobrunr) — rich job framework
  (built-in dashboard, state machine, listener pattern).

## License

See [LICENSE](LICENSE).
