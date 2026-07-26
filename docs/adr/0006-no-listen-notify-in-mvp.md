# ADR-0006: LISTEN/NOTIFY removed from MVP

## Status

Accepted — amended 2026-07-26 (same-JVM after-commit wake-up implemented,
replacing the never-built `afterDone` mitigation; see the Amendment
section at the bottom)

## Date

2026-04-20

## Context

PostgreSQL offers a built-in NOTIFY/LISTEN mechanism for push notifications:
a subscriber holds a long-lived connection running `LISTEN channel`, while
a trigger on INSERT/UPDATE calls `pg_notify(channel, event_type)`. The
notification reaches the subscriber after COMMIT.

For event-outboxer this could reduce publish→handle latency to tens of
milliseconds, compared with `pollingInterval/2` (~5 seconds with
polling=10s).

Initially we proposed to include this as an opt-in feature in MVP through an
`EventStorePushSource` port. During discussion it became clear that it is
not a good fit for MVP.

## Alternatives considered

- **A. Include in MVP, default=false**: add an SPI port, a PG implementation
  via a trigger + LISTEN connection; enabled through configuration.
- **B. Include in MVP, default=true**: sub-second latency out of the box.
- **C. Remove from MVP entirely**: neither port nor implementation. We rely
  only on adaptive polling + `afterDone` callback wake-up.

## Decision

**Option C was chosen**: remove it from MVP entirely:
- No `EventStorePushSource` port in SPI.
- No trigger migration in
  `storage-postgres/src/main/resources/db/migration/outbox/notify/`.
- No LISTEN connection or daemon thread.

**MVP polling architecture**:
1. Timer-based polling per event type with adaptive backoff on empty cycles
   (`pollingInterval` doubles up to `maxIdlePollingInterval` after N empty
   cycles; the first non-empty cycle resets to the base).
2. Wake-up via an `afterDone` callback from the worker thread: when
   `currentlyInFlight <= lowerLimit && moreEventsInDB`, wake the poller via
   `Waiter.wakeOrSkipNextWait()`.

## Rationale

### pgbouncer caveat

pgbouncer in `transaction pooling` mode **does not support LISTEN** — the
connection returns to the pool after every TX, and PostgreSQL loses the
subscription. This is a very common production setup:
- AWS RDS Proxy.
- Managed PostgreSQL clusters with a pooler.
- Custom pgbouncer deployments at medium/large scale.

If LISTEN were enabled by default, users behind pgbouncer would face
**silent broken behavior**: the subscription does not register, but polling
still works → no outage, yet the promised low latency does not materialize.
An implicit mismatch between the advertised and actual behavior.

### Hidden dependencies

- A separate JDBC connection, long-lived, bypassing the connection pool.
- A daemon thread for the `getNotifications` loop.
- Reconnect logic on connection failure.
- Edge cases: overflow of the notification queue, races between commit and
  notification delivery.

Each of these pieces is a source of long-term bugs. For MVP it is excess
surface area.

### Adaptive polling delivers acceptable latency

During active traffic, the `afterDone` callback wakes the poller immediately
after a handler finishes, so the next batch is picked up right away.
Observed publish→handle latency is hundreds of milliseconds, not 5 seconds.

Sub-100ms latency is a rare requirement for outbox use cases. If it becomes
a real need, LISTEN/NOTIFY can be reintroduced later as an opt-in feature.

### YAGNI

"It's unclear how much it is needed" — a direct argument. We add the
feature once a real use case emerges.

## Consequences

### For users

- MVP provides publish→handle latency in the range 100 ms – `pollingInterval`
  (10 s default).
- If stricter sub-second latency is required, configure
  `polling-interval: 1s` or smaller.
- Transaction-mode pgbouncer requires no special workarounds.

### For maintainers

- The core does not include an SPI for push notifications. If one is needed,
  it will be added in a later release (possibly as a breaking SPI change).
- The PG adapter does not have a trigger migration.
- Documentation carries an explicit note about the possible post-MVP
  feature.

### Positive consequences

- Simpler. Less code, fewer bugs.
- Works with any pgbouncer setup.
- No extra JDBC connection per JVM.
- Adaptive polling covers latency adequately in most cases.

### Negative consequences

- Worst-case latency equals `pollingInterval`. With `polling-interval: 10s`
  a freshly published event could take 5–10 seconds to be handled.
- For applications with strict latency SLAs this may be problematic →
  reduce `polling-interval`, increasing DB load.

## Post-MVP path

If real demand emerges:

1. Introduce an `EventStorePushSource` port in SPI:
   ```java
   public interface EventStorePushSource {
       AutoCloseable subscribe(String eventType, Runnable wakeupCallback);
   }
   ```
2. Add a separate migration `notify/V002__outbox_notify.sql` with the
   trigger.
3. Implement `PostgresEventStorePushSource` in `storage-postgres`:
   dedicated connection + `LISTEN` loop + reconnect + `wakeupCallback`
   routing.
4. Auto-configuration via `@ConditionalOnClass` + `@ConditionalOnProperty`.
5. Documentation — explicit warning about transaction-mode pgbouncer.

## Amendment (2026-07-26): same-JVM after-commit wake-up

Two corrections to the original text.

**1. The `afterDone` mitigation was never built.** The Decision section
above cites "Wake-up via an `afterDone` callback ... via
`Waiter.wakeOrSkipNextWait()`" and the Rationale claims "observed
publish→handle latency is hundreds of milliseconds". No such mechanism
existed in the implementation: `AdaptiveWaiter` had no wake method and
`Poller` parked unconditionally, so the real latency floor was
`pollMinInterval..pollMaxInterval` (500 ms – 10 s), and after an idle
period — the ceiling.

**2. It has been replaced by a publish-side after-commit wake-up**, which
captures most of LISTEN/NOTIFY's benefit while defeating every objection
this ADR raised against it (no extra connection, no daemon thread, no
pgbouncer caveat):

- `TransactionContext` gained `default void afterCommit(Runnable)`;
  the Spring implementation defers the action through
  `TransactionSynchronizationManager.registerSynchronization`, so it runs
  only after a real commit and never on rollback.
- `DefaultOutboxEventPublisher` registers a hook that wakes the local
  poller(s) of the just-published event type(s) via `PollerWakeHub` →
  `Poller.wake()` (`LockSupport.unpark`).
- Purely an optimization: polling remains the correctness mechanism; a
  missed wake merely costs one poll interval.

Resulting latency profile:

- **Same JVM (the common embedded-outbox case, ADR-0001)**: publish→handle
  is bounded by the handler, not the poll interval — milliseconds.
- **Cross-pod** (another instance claims the event) and delayed events
  (`runAt` in the future): still poll-bound, `pollMinInterval..pollMaxInterval`.

The Post-MVP path below (LISTEN/NOTIFY as an opt-in push source for
cross-pod latency) remains valid and unchanged.

## Related decisions

- [ADR-0004](0004-per-event-type-worker-isolation.md) — per-type polling
  model.
- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — a push-source port
  can later slot into the SPI without touching the core.
