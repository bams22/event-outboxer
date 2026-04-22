# ADR-0003: Payload is an explicit DTO, not a lambda

## Status

Accepted

## Date

2026-04-19

## Context

jobrunr's iconic API looks like this:
```java
BackgroundJob.enqueue(() -> emailService.sendConfirmation(orderId));
```

The lambda is serialized via `SerializedLambda` + ASM + reflection, stored in
the DB, and deserialized in the consumer. This yields a minimally verbose
API, but introduces a deep problem — **hidden coupling between stored data
and code structure**.

We must decide whether to adopt this model or to use explicit DTOs.

## Alternatives considered

- **A. Lambda serialization (as in jobrunr)**: API
  `publisher.enqueue(() -> svc.doWork(id))`. The lambda captures the
  arguments, is turned into `{className, methodName, signature,
  capturedArgs}`.
- **B. Explicit DTO**: API `publisher.publish("TYPE", new TypePayload(...))`.
  Payload is an ordinary Java record / POJO, serialized to JSON via Jackson.

## Decision

**Option B was chosen**: payload is an explicit DTO serialized as JSON.
Lambda-based APIs are explicitly unsupported. The public API does not contain
`Runnable`, `Consumer`, or lambda-based signatures.

## Rationale

Lambda serialization's main problems:

1. **Refactoring breaks running jobs**. Renaming a method / reordering
   parameters / changing an argument type causes all previously stored jobs
   for that method to fail at runtime during deserialization. The compiler
   does not see it. Tests pass. Example: was `sendEmail(String email)`,
   became `sendEmail(String email, String locale)` — old jobs explode when
   processed.

2. **Rolling deploy creates an irrecoverable poisoning window**. Pod A on
   new code stores a new lambda; Pod B on old code picks it up and cannot
   deserialize. During rollout the queue is poisoned.

3. **The lambda captures the enclosing `this`**. Non-static lambdas drag a
   reference to the bean, with all of its state, into the payload. Behavior
   is hard to predict when the bean changes.

4. **Unreadable in the DB**. The cheapest way to debug the outbox is
   `SELECT payload FROM event_outboxer.events WHERE id=...`. A lambda is a binary
   sandwich with metadata; you cannot tell "email for Peter Ivanov" just by
   looking at it.

5. **Security**. Any deserialization surface with an arbitrary `className`
   is a potential gadget chain. Closing that requires an allow-list of
   classes, which is itself a source of bugs.

6. **Bytecode coupling**. ASM parsing of `writeReplace()` and
   `SerializedLambda` is sensitive to JDK and compiler versions (javac vs
   kotlinc vs ECJ, JDK17 → 21 → 25).

Explicit DTOs close all of that:

- **Evolution** via DTO versioning:
  `@JsonIgnoreProperties(ignoreUnknown=true)`, defaults for new fields — all
  standard practices, well covered by compatibility tests.
- **Handler** is a separate class bound to `eventType` as a string.
  Refactoring the handler does not break the queue.
- **Security**: the allow-list is narrowed down to a single
  `handler.payloadType()` — the handler knows what it deserializes, there
  are no other options.
- **Readability**: JSONB directly in the DB,
  `SELECT payload FROM event_outboxer.events` returns human-readable content.

## Consequences

### For users

- API: `publisher.publish("EVENT_TYPE", new EventPayload(...))` — a string
  type name and a DTO object.
- Each event type has a dedicated payload class (record or POJO).
- Handlers are bound to payloads via `eventType()` (String) and
  `payloadType()` (Class), not by method signatures.
- Collections should be wrapped in a DTO
  (`record BulkOrdersPayload(List<Order> items) {}`), see
  [ADR-0011](0011-jackson-json-only-in-mvp.md).

### For maintainers

- No `Runnable`, no `Consumer<?>`, no action-style parameters in the API.
- Sugared APIs like `publisher.run(() -> ...)` are architecturally
  forbidden.
- Any proposal to add that sugar is rejected with a reference to this ADR.

### Positive consequences

- Refactoring-safe.
- Safe rolling deploys.
- SQL-friendly debugging.
- Security — minimal deserialization surface.

### Negative consequences

- The API is slightly more verbose: `publisher.publish("TYPE", new
  Payload(...))` vs `publisher.run(() -> svc.doWork(id))`. This is a
  deliberate cost.

## Related decisions

- [ADR-0011](0011-jackson-json-only-in-mvp.md) — Jackson JSON as the
  serialization format.
- [ADR-0012](0012-extract-lock-key-on-handler.md) — `lockKey` is derived
  from the payload inside the handler.
