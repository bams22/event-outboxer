# ADR-0015: At-least-once semantics with idempotent handlers

## Status

Accepted

## Date

2026-04-20

## Context

In distributed systems, the choice between exactly-once, at-least-once, and
at-most-once is a fundamental architectural decision that determines what
users should expect and what guarantees we can provide.

- **Exactly-once** — the ideal, but theoretically impossible in general
  (two-generals problem + failure scenarios). Approximations require
  distributed transactions (2PC) or special brokers such as Kafka with
  transactional writes.
- **At-most-once** — data loss is acceptable. Unacceptable for an outbox.
- **At-least-once** — no events are lost, but duplicates are possible.
  The handler must be idempotent.

## Decision

**We choose at-least-once** as an explicit contract.

Every event will be processed **at least once**. Possibly more. The user's
handler **MUST** be idempotent.

## Rationale

### Duplicate-producing scenarios

1. **Worker successfully ran the handler, crashed before finalize**.
   Example:
   ```
   handler.handle() → success
   [worker dies: network between handler and DB, kill -9, OOM]
   → finalize not executed, event remains in PROCESSING
   → after deadThreshold, orphan recovery returns it to PENDING
   → another worker processes it again
   ```

2. **False-positive orphan recovery**: a long GC stall on a worker (>
   deadThreshold). Its events are reclaimed, yet the worker later wakes up
   and finishes. Its `markProcessed` returns false (version already
   changed), but the new worker processes anyway.

3. **False-positive watchdog**: `handlerMaxRuntime` is too small but the
   handler is legitimately long. Watchdog force-reclaims → a new worker
   processes → the old worker finishes but its finalize returns false.

4. **Retry after a transient exception** that already applied a side
   effect: `apiCall()` returned Timeout, yet on the remote side the
   operation succeeded. `handle()` threw → Retry → the next attempt
   applies the operation a second time.

5. **Network partition during commit**: `markProcessed` committed to the
   DB, but the acknowledgement did not reach the worker. The worker thinks
   it failed and retries. (Genuinely rare but possible.)

### Why not exactly-once

- Requires distributed transactions between the DB and the handler's
  side effects (API call, email, Kafka publish). Impossible in the general
  case.
- 2PC/XA exists but performs poorly; the coordinator is a SPOF.
- Transactional broker integration (Kafka transactions) works for
  broker-only flows, it does not cover arbitrary side effects.

### Why not at-most-once

- The outbox exists precisely to **guarantee** events. Losing events
  contradicts the reason for its existence.
- A handler crashing without retry means loss. Unacceptable.

### At-least-once is the only sensible choice

- Preserves the guarantee: the event will be processed.
- Cost: the handler must be idempotent.
- Idempotency is a standard distributed-systems pattern that does not
  require heroic effort from handlers.

## Idempotency patterns for handlers

Recommended patterns:

### 1. Naturally idempotent operations

Some operations are naturally idempotent:
- `SET user.email = 'new@ex.com' WHERE user.id = X` — repeating does not
  change the outcome.
- `PUT /api/orders/123 { "status": "SHIPPED" }` — REST PUT.

### 2. Idempotency key (Idempotency-Key header)

For remote API calls that are not naturally idempotent (e.g. POST):
```java
public EventOutcome handle(EventContext ctx, SendEmailPayload p) {
    emailApi.send(p.email(), p.content(),
                  headers("Idempotency-Key", ctx.id().toString()));
    return Success.INSTANCE;
}
```
The remote side checks whether a request with that key has already been
received. Repeats become no-ops.

### 3. Check-then-act

```java
public EventOutcome handle(EventContext ctx, UpdateOrderPayload p) {
    Order order = orderRepo.findById(p.orderId()).orElseThrow();
    if (order.status() == SHIPPED) {
        return Skip.INSTANCE;   // already processed
    }
    order.markShipped();
    orderRepo.save(order);
    return Success.INSTANCE;
}
```

### 4. `@Transactional` + business rules

If the handler works with the DB: `@Transactional` + a pre-write check:
```java
@Transactional
public EventOutcome handle(EventContext ctx, DebitAccountPayload p) {
    if (transactionRepo.existsByRefId(ctx.id())) {
        return Skip.INSTANCE;  // already applied
    }
    accountService.debit(p.accountId(), p.amount());
    transactionRepo.save(new Transaction(ctx.id(), ...));
    return Success.INSTANCE;
}
```

### 5. Dedup table

For handlers where idempotency is hard to achieve, use a dedicated table
`event_processed(event_id PRIMARY KEY, processed_at)`:
```java
if (processedRepo.existsById(ctx.id())) return Skip.INSTANCE;
doWork(payload);
processedRepo.save(new Processed(ctx.id(), now()));
```

(Everything in a single TX so that the work and the dedup write commit
atomically.)

## Consequences

### For users

- **Obligation**: handlers must be idempotent. This is called out in the
  `EventHandler.handle()` javadoc and in the README.
- The documentation contains a section on idempotency patterns (this ADR
  or a standalone guide).
- The metric `outbox.events.concurrent_completion_conflict` shows how often
  race situations arise. Zero means no duplicates. Non-zero means the
  handler MUST tolerate it.

### For maintainers

- **We do NOT try to guarantee exactly-once**. Any "we think we can"
  suggestions are rejected.
- Every concurrency decision is checked against: "what if this happens
  twice?" — if nothing catastrophic occurs (under idempotency), it is OK.
- The contract is advertised everywhere: in javadocs, the README, the
  handler docs.

### Positive consequences

- Simplicity: no heroic efforts for unachievable exactly-once.
- Production-tested: at-least-once is a time-honored model in distributed
  systems.
- Clear contract: users know what they are responsible for.

### Negative consequences

- The handler must be idempotent — not always trivial (you need to reason
  about natural idempotency or introduce a dedup mechanism).
- In the worst case (handler NOT idempotent) — double side effects: an
  email sent twice, money deducted twice. This is the user's
  responsibility.

## Related decisions

- [ADR-0014](0014-optimistic-locking-via-version-field.md) — the
  concurrency mechanism that produces at-least-once races.
- [ADR-0005](0005-workers-heartbeat-table.md) — orphan recovery, one of
  the causes of duplicates under false positives.
- [ADR-0007](0007-failure-handler-chain-of-responsibility.md) — retry
  after a transient exception is another cause.
