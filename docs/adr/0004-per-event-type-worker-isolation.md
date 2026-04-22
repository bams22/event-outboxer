# ADR-0004: Per-event-type worker isolation

## Status

Accepted

## Date

2026-04-19

## Context

db-scheduler uses **a single shared pool** (`ExecutorService executorService`)
for all task types, and **a single shared poller** (`ExecutorService
dueExecutor`) that polls all types together.

This works well for a scheduler: tasks are roughly similar — all background,
with similar latency profiles, the differences are small.

For outbox use cases, events have very different characteristics:
- `UPDATE_CACHE`: 5ms, frequent, latency-sensitive.
- `SEND_EMAIL`: 400ms, through an external SMTP.
- `DAILY_REPORT`: minutes, background, rare.

With a shared pool, a slow handler blocks the queue of fast ones, and a poll
cycle for one type delays the polling of others. We must decide how to
isolate them.

## Alternatives considered

- **A. Shared pool + shared poller** (db-scheduler style): simpler, fewer
  threads, but no isolation.
- **B. Per-event-type executor + shared poller**: isolates processing, but
  the poller is still shared — a large backlog on one type can delay
  discovery of events of other types.
- **C. Per-event-type executor + per-event-type poller**: full isolation;
  one type does not affect another at the claim stage or the processing
  stage.

## Decision

**Option C was chosen**: every registered `EventHandler<T>` gets:

1. Its own `ThreadPoolTaskExecutor` (Spring-managed) with the thread prefix
   `outbox-handler-${eventType}-`.
2. Its own poller thread (single-thread executor with the prefix
   `outbox-poller-${eventType}`) with its own `Waiter`.
3. Its own claim query to the DB:
   `SELECT ... WHERE event_type = ? AND status='PENDING' AND run_at <=
   now()`.

Shared across all types:
- DataSource + connection pool.
- Maintenance executor (heartbeat, orphan recovery, watchdog).
- `EventStore`, `WorkerRegistry`, `EventSerializer`.

## Rationale

- **SLA isolation**: a slow `SEND_EMAIL` does not block a fast
  `UPDATE_CACHE`. Each type has its own queue and its own backpressure.
- **Backlog isolation**: 1M events of type A do not prevent the poller for
  type B from discovering new B events. With a shared poller, a single
  `SELECT FOR UPDATE SKIP LOCKED` could return only A events until the limit
  is exhausted.
- **Per-type tuning**: `handler-pool-size`, `handler-queue-capacity`,
  `polling-interval` are configured per type (with inheritance from
  `defaults`).
- **Observability**: metrics such as `outbox.executor.active{event_type=X}`
  and `outbox.queue.size{event_type=X}` are naturally scoped.
- **Proven pattern**: this pattern is known to work well in practice for
  outbox workloads; it keeps each event type's pipeline truly independent.

## Consequences

### For users

- Per-type configuration through `outbox.handlers.types.<eventType>.*` in
  application.yml.
- With many handlers (30+), the total thread count grows. Conservative
  defaults (`handler-pool-size=3, handler-queue-capacity=100`) are
  recommended; only busy types should be scaled up.

### For maintainers

- The engine builds a per-type triad (`poller + executor + dispatcher`) at
  startup by iterating over all `EventHandler<?>` beans.
- The partial index `(event_type, priority DESC, run_at) WHERE
  status='PENDING'` is critical — every poller performs a seek on
  `event_type = ?`.

### Positive consequences

- One slow type does not poison the queue.
- Independent per-type tuning.
- Precise observability.
- Scaling — a new handler yields a new independent pipeline.

### Negative consequences

- Many threads with a large N of handlers (mitigated by conservative
  defaults; the handler pool is fixed-size per type, so tuning means
  choosing `handler-pool-size` per expected concurrency).
- N pollers × `polling-interval` → N queries/sec to the DB at idle. Mitigated
  by adaptive backoff after N consecutive empty polls.
- Slightly more memory for registries and stopped states.

## Related decisions

- [ADR-0009](0009-spring-task-executor-in-starter.md) — the executors are
  Spring `ThreadPoolTaskExecutor` instances with a `TaskDecorator`.
- [ADR-0005](0005-workers-heartbeat-table.md) — heartbeat is shared per JVM,
  not per type.
