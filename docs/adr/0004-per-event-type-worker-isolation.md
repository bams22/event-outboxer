# ADR-0004: Per-event-type worker isolation

## Status

Accepted — amended 2026-07-26 (claiming is now capacity-coupled to the
per-type executor) and 2026-08-29 (the refill is gated on a free-capacity
watermark); see the Amendment sections at the bottom

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
- **Observability**: metrics such as `event-outboxer.executor.active{event_type=X}`
  and `event-outboxer.queue.size{event_type=X}` are naturally scoped.
- **Proven pattern**: this pattern is known to work well in practice for
  outbox workloads; it keeps each event type's pipeline truly independent.

## Consequences

### For users

- Per-type configuration through `event-outboxer.handlers.types.<eventType>.*` in
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

## Amendment (2026-07-26): capacity-coupled claiming

The original per-type pipeline claimed a fixed `claimBatchSize` on a
fixed timer, with no feedback from the executor. That had two flaws:

1. **A throughput ceiling independent of the pool size** —
   `claimBatchSize / pollMinInterval` (20 events/s with defaults) per
   type per pod, no matter how large `handler-pool-size` was.
2. **Claim/release churn under overload** — with the pool and queue
   full, every poll claimed rows (`UPDATE`, `version++`) whose
   dispatches were rejected and released back (`UPDATE`, `version++`):
   two wasted hot-table writes per unit of useful work.

The poller and its executor are now coupled through
`HandlerExecutorGate` (implemented by `HandlerExecutorManager`'s
per-type slot, which tracks in-flight tasks against a budget of
`handlerPoolSize + handlerQueueCapacity`):

- **Capacity-aware claim**: each poll claims
  `min(claimBatchSize, freeCapacity)`; at zero free capacity the poller
  does not claim at all.
- **Full-batch immediate re-poll**: a poll that returns a full batch
  re-polls immediately (bounded by free capacity), so sustained
  throughput is limited by the handlers and the database, not by the
  poll timer.
- **Capacity-available wake**: the executor's saturated→free transition
  wakes the poller (`Poller.wake()`, shared with the after-commit wake
  of ADR-0006), so "handler finished → next event claimed" costs
  microseconds instead of up to `pollMinInterval`. This is the
  `afterDone` mechanism the original ADR-0006 text described but never
  built.

For the virtual-thread executor flavour (`handler-executor.type:
virtual`, ADR-0017) the same budget acts as a soft in-flight cap —
previously `handler-pool-size`/`handler-queue-capacity` were ignored
for virtual executors, leaving in-flight growth unbounded.

Dispatch rejection (`RejectedExecutionException` → release back to
PENDING without an attempts bump) remains as a safety net for capacity
races, but is no longer a steady-state occurrence.

## Amendment (2026-08-29): watermark-gated refill

The capacity-coupled claim above tops the executor up as soon as a
single slot frees: the capacity-available wake fires on the
saturated→free edge and the poller claims `min(claimBatchSize, 1)`.
Under sustained load that is **one claim statement per handler
completion** regardless of `handlerQueueCapacity` — the queue only
decides how many rows this JVM holds in `PROCESSING` ahead of time (see
CONFIGURATION.md §Sizing the handler queue); it never amortises claims.
The natural batching that does occur (several completions landing
before the poller wakes, claimed together) is incidental and vanishes
exactly when handlers are slow enough for the queue to matter.

`EventTypeConfig.claimMinFree` (default `1`, i.e. the behaviour above
unchanged) turns the queue into a real prefetch:

- **Watermark-gated claim**: the poller claims only when
  `freeCapacity >= claimMinFree`, and the check runs on every loop
  iteration — a partial batch that drops free capacity below the
  threshold waits for the threshold again instead of trickling one-row
  claims on the adaptive timer. Below the threshold the poller parks
  exactly as it does when saturated (bounded fallback of
  `pollMinInterval`); `onPollerSaturated` still fires only at zero free
  capacity.
- **Threshold wake**: `HandlerExecutorGate.onCapacityAvailable` fires on
  the edge where free capacity reaches `claimMinFree` (the
  saturated→free edge when it is 1). Completions decrement in-flight one
  at a time, so the edge cannot be skipped.
- **Bounds**: `1 <= claimMinFree <= handlerPoolSize +
  handlerQueueCapacity`, validated in the record. With a platform
  executor a threshold above `handlerQueueCapacity` idles handler
  threads while the poller waits for the refill — the starter warns at
  startup. For the virtual-thread executor, where the budget is a soft
  in-flight cap and there is no queue, the same setting is a deliberate
  concurrency-for-batching trade and stays silent.

With `handlerPoolSize 3`, `handlerQueueCapacity 30`, `claimMinFree 30`
and `claimBatchSize 30` the type claims once per 30 events instead of
once per event; the worst-case wait of the last claimed row
(`(pool + queue) / pool × t_handler`) and the stale-claim bound are
unchanged.

What the amendment does not change: the in-flight budget and its
hoarding semantics, the after-commit wake (a locally published event
still wakes the poller, which then applies the threshold — under load
the event would have queued behind the prefetched rows anyway, and
waiting in the store instead of the local FIFO lets `priority` order it
correctly), and the light-load regime — with the store nearly empty the
poller keeps polling on the adaptive timer whenever free capacity is at
or above the threshold; batching there is a latency trade governed by
`pollMinInterval`, not by this setting.

## Related decisions

- [ADR-0009](0009-spring-task-executor-in-starter.md) — the executors are
  Spring `ThreadPoolTaskExecutor` instances with a `TaskDecorator`.
- [ADR-0005](0005-workers-heartbeat-table.md) — heartbeat is shared per JVM,
  not per type.
- [ADR-0006](0006-no-listen-notify-in-mvp.md) — the publish-side
  after-commit wake; together with the capacity wake it bounds latency
  at both ends of the pipeline.
