# ADR-0012: extractLockKey() on the handler

## Status

Accepted — amended 2026-07-26 (per-backend guarantees made explicit,
lockTtl >= handlerMaxRuntime now enforced; the PublishOptions.lockKey
mention below was contradictory and is corrected; see the Amendment
section at the bottom)

## Date

2026-04-20

## Context

Some events require processing to be serialized by a business key (so that
two events of the same aggregate are not processed in parallel). For
example, `UPDATE_ORDER_STATUS` and `ADD_ORDER_ITEM` for the same Order must
lock on `"order-42"` — otherwise the business logic races.

The key question is: **where is the `lockKey` defined** and **where is it
stored**?

Three options were considered:

1. **An interface on the payload** (`LockableEvent { String lockKey(); }`).
2. **A field in `PublishOptions`** + a `lock_key` column in the DB.
3. **`extractLockKey()` on the handler**; the payload stays clean; the
   `lockKey` is computed on the fly on the worker after deserialization;
   **it is not stored in the DB**.

## Alternatives considered

- **A. `LockableEvent` interface on the payload**:
  - ➕ `lockKey` "cannot be forgotten" (compile-time check).
  - ➖ Couples a user DTO to a library-specific interface.
  - ➖ Rigid: one payload = one `lockKey`; different scenarios are not
    possible.

- **B. `PublishOptions.lockKey` + `lock_key` column in the DB**:
  - ➕ `lockKey` is visible in the DB, can be queried for observability.
  - ➕ The lock is acquired BEFORE deserialization (optimization on busy
    locks).
  - ➕ Pluggable — the payload may be any DTO.
  - ➖ The publish API becomes cluttered with options.

- **C. `extractLockKey()` on the handler, not stored in the DB**:
  - ➕ The publish API is pristine (`publisher.publish(type, payload)`).
  - ➕ The payload stays a "clean DTO" without library-specific interfaces.
  - ➕ `extractLockKey` lives next to the handler code (semantically
    correct).
  - ➕ The publisher is independent of the handler resolver (cross-service
    producers work without a handler registry).
  - ➖ The lock is acquired AFTER deserialization (small overhead on busy
    locks).
  - ➖ `lockKey` is not visible in the DB (no SQL-based observability).

## Decision

**Option C was chosen**.

### API

```java
public interface EventHandler<T> {
    String eventType();
    Class<T> payloadType();
    EventOutcome handle(EventContext ctx, T payload);

    default String extractLockKey(T payload) {
        return null;  // null = no lock applied
    }
}
```

### Usage example

```java
@Component
public class UpdateOrderStatusHandler
        implements EventHandler<UpdateOrderStatusPayload> {

    @Override public String eventType()         { return "UPDATE_ORDER_STATUS"; }
    @Override public Class<UpdateOrderStatusPayload> payloadType() {
        return UpdateOrderStatusPayload.class;
    }

    @Override
    public String extractLockKey(UpdateOrderStatusPayload payload) {
        return "order-" + payload.orderId();
    }

    @Override
    public EventOutcome handle(EventContext ctx, UpdateOrderStatusPayload payload) {
        // the handler knows nothing about the lock — it has already been
        // acquired (or not) by the core engine BEFORE this call
        orderService.updateStatus(payload.orderId(), payload.status());
        return EventOutcome.Success.INSTANCE;
    }
}
```

### Worker-side flow

```java
void processEvent(ClaimedEvent event) {
    EventHandler<Object> handler = resolver.resolve(event.eventType()).orElseThrow();
    Object payload = serializer.deserialize(event.payload(), handler.payloadType());

    String lockKey = handler.extractLockKey(payload);
    if (lockKey != null) {
        Optional<LockHandle> lockOpt = locker.tryLock(lockKey, lockTtl);
        if (lockOpt.isEmpty()) {
            markForRetry(event, "entity locked: " + lockKey, lockBusyDelay);
            return;   // ← the handler is NOT invoked
        }
        try (LockHandle lock = lockOpt.get()) {
            invokeHandler(handler, event, payload);
        }
    } else {
        invokeHandler(handler, event, payload);
    }
}
```

**The handler knows nothing about the lock** — all lock handling lives in
the core engine.

### Schema impact

- No `lock_key` column in the DB.
- No `lockKey` field on `PendingEvent`/`ClaimedEvent`/`Event`.
- No `lockKey` field on `PublishOptions`.

## Rationale

### Why a clean API trumps lock-key observability

We deemed observability by `lockKey` low value. In practice:
- The `lockKey` is visible inside the payload (by `orderId`, `userId`,
  etc.) — you can pull it via a JSONB query:
  `WHERE payload->>'orderId' = '42'`.
- "How many events for Order#42" analytics through a JSONB query works.
- An additional column would duplicate information for a rare use case.

### Why redundant deserialization is acceptable

Outbox payloads are typically small (1 business operation ≈ 5–10 fields).
Jackson deserialization of that size is microseconds. Even under heavy
lock contention (many busy events) the overhead is negligible.

### Why not option A (LockableEvent interface)

The main downside is coupling a user DTO to a library type. This breaks the
hexagonal principle "domain does not depend on infrastructure". Today the
DTO is used only in the outbox; tomorrow it appears in a REST API or Kafka
message — and the event-outboxer dependency creeps into the API module.

### Why not the hybrid (option B + extractLockKey)

A hybrid (publisher resolves the handler and stores `lockKey` in the DB)
would give:
- A clean publish API ✓
- `lockKey` in the DB ✓
- Lock acquired before deserialization ✓

But: the publisher becomes dependent on the handler resolver. For
cross-service scenarios (producer-only without handlers) this breaks
isolation. Since the flexibility was not considered valuable, the simpler
option was chosen.

## Consequences

### For users

- Publish API: `publisher.publish(type, payload)` — no options in 99% of
  cases.
- The lock key comes exclusively from `EventHandler.extractLockKey(payload)`
  — there is deliberately no `PublishOptions.lockKey` escape hatch
  (§Schema impact above; an earlier draft of this section referenced a
  builder method that never existed).
- Handlers can declare `extractLockKey(payload)` to serialize processing
  per aggregate.
- The handler itself does not deal with `LockHandle`.

### For maintainers

- `DefaultHandlerDispatcher` calls `handler.extractLockKey(payload)` after
  deserialization and before `handler.handle()`.
- The publisher is independent of handlers.
- No `lock_key` column in the DB schema.

### Positive consequences

- Cleanest API: `publish(type, payload)`.
- Clean payload: no library interfaces.
- Thin publisher: serialize + save.
- Self-contained handler: defines eventType, payload type, lock strategy.

### Negative consequences

- The lock is acquired after deserialization (micro overhead on busy
  locks).
- `lockKey` is not directly visible as a column in the DB (but can be
  extracted from the payload via JSONB queries).

## Amendment (2026-07-26): guarantees made explicit and enforced

The original text promised "serialize processing per aggregate"
unconditionally. The real guarantees differ per backend and are now
documented and partially enforced:

| Backend | Exclusion holds until | Crash release | Cost |
|---|---|---|---|
| Redis/KeyDB (`SET NX PX` + token-checked release) | `min(close(), ttl)` | TTL expiry (≤ ttl) | one Redis key |
| PostgreSQL advisory (session-scoped) | `close()` or connection loss | connection drop, immediate | **one pooled connection per held lock** |

Consequences and the measures added:

1. **`lockTtl >= handlerMaxRuntime` is enforced** in
   `EventTypeConfig` (startup failure otherwise), and the default
   `lockTtl` was raised from 5m (equal to the handler budget — a race
   at the boundary) to 10m (2×). Recommendation: keep ≥ 2× — the
   margin covers a zombie handler finishing after its claim was
   force-reclaimed by the watchdog.
2. **Redis is best-effort without fencing.** The watchdog does not
   interrupt handler threads; a zombie running past
   `handlerMaxRuntime` can outlive the TTL and overlap the next
   holder. Full exclusion under arbitrary stalls requires fencing
   tokens checked at the protected resource — out of scope (the
   classic distributed-locks argument). Lock renewal (heartbeat-style
   TTL extension) remains a Post-MVP option.
3. **The PG locker's pool coupling is now visible.** Each held lock
   pins one connection of the application's shared pool; with
   `Σ handlerPoolSize >= maximum-pool-size` a saturated fleet can
   deadlock against its own handlers (locker holds the last
   connection, handler waits for one). The starter logs a startup
   WARNING when the sums line up that way; the fix is a larger pool
   or smaller handler pools.

## Related decisions

- [ADR-0003](0003-explicit-dto-payload.md) — the payload is an explicit
  DTO, which is what allows `extractLockKey` to be called on a typed
  object.
- [ADR-0011](0011-jackson-json-only-in-mvp.md) — Jackson's cheap
  deserialization makes acquiring the lock after deserialization
  acceptable.
