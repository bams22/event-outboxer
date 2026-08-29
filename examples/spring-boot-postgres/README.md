# event-outboxer example — Spring Boot + PostgreSQL

Minimal demo showing the `@Transactional createOrder(...)` → `publisher.publish(...)` →
`SendOrderConfirmationHandler` pipeline.

## Prerequisites

- JDK 25 or newer (library baseline)
- Maven 3.9+ (or use the wrapper from the parent project)
- Docker for the `docker-compose.yml` services (PostgreSQL 15, KeyDB 6)

## Run

```bash
cd examples/spring-boot-postgres

# 1. Start Postgres (and KeyDB for later experiments):
docker compose up -d

# 2. Launch the app:
../../mvnw spring-boot:run
```

The application listens on `http://localhost:8080`. Create an order:

```bash
curl -X POST http://localhost:8080/orders \
  -H 'content-type: application/json' \
  -d '{"customerId":"cust-1","email":"me@example.com","totalCents":1999}'
```

The response contains the new order id. Within a second or so the handler logs:

```
sending confirmation for order <id> to me@example.com (attempt 1)
```

## Try the rollback guarantee

Add `orderRepository.save(null)` after the `publisher.publish(...)` call in
`OrderService.createOrder(...)` — the transaction rolls back and **no event is
persisted**. The handler is not triggered. Verify with:

```sql
SELECT count(*) FROM event_outboxer.events;
SELECT count(*) FROM orders;
```

## Useful endpoints

- `http://localhost:8080/actuator/health/outbox` — engine state + backlog.
- `http://localhost:8080/actuator/prometheus` — Micrometer metrics
  prefixed `event_outboxer.*`, one `event_type` tag per per-event signal.

Field-level reference, the full metric catalogue and a troubleshooting
playbook live in [docs/OBSERVABILITY.md](../../docs/OBSERVABILITY.md).

## Explore the schema

```bash
docker exec -it outboxer-example-pg psql -U outbox
```

```sql
\dt event_outboxer.*   -- events, workers, event_archive, entity_locks + the outbox's
                       -- own flyway_schema_history (ADR-0028); public.flyway_schema_history
                       -- holds only the application's V001__orders
SELECT event_type, status, attempts, run_at
  FROM event_outboxer.events
  ORDER BY created_at DESC LIMIT 5;
SELECT * FROM event_outboxer.workers;
```
