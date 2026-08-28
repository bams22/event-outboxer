# ADR-0007: FailureHandler chain-of-responsibility

## Status

Accepted — amended 2026-07-26 (deserialization failures now route
through the chain) and 2026-08-29 (YAML binding lives under
`event-types`, precedence fixed by ADR-0030); see the Amendment
sections at the bottom

## Date

2026-04-20

## Context

When `handler.handle()` returns `Retry`/`Fail` or throws an exception, we
need to decide what to do with the event: reschedule (retry), move to
DISABLED, or delete. The decision depends on multiple concerns: how many
attempts have already been made, which backoff strategy to use, whether to
log, whether to emit a metric.

A simple `RetryPolicy` was initially proposed:

```java
public interface RetryPolicy {
    Instant nextAttemptAt(int attempts, Instant now);
    int maxAttempts();
    ExhaustedAction onExhausted();  // DISABLE | DELETE
}
```

It covers the basic case (exp backoff, limit, disable) but does not allow
orthogonal composition of logging, metrics, or conditional logic (e.g. a
different strategy depending on the exception type).

db-scheduler uses a composable pattern:
```java
new LogFailureHandler<>(
    new MaxRetriesFailureHandler<>(10,
        new ExponentialBackoffFailureHandler<>(Duration.ofSeconds(5), 2.0)))
```

Each decorator adds behavior; the leaf handler makes the final decision
(RetryAt / Disable / Delete).

## Alternatives considered

- **A. Simple `RetryPolicy`**: only exp/fixed backoff + maxAttempts +
  exhaustedAction.
- **B. Chain-of-responsibility `FailureHandler<T>`**: composable decorators.

## Decision

**Option B was chosen**: `FailureHandler<T>` chain fully replaces
`RetryPolicy`.

### API

```java
public interface FailureHandler<T> {
    FailureDecision onFailure(FailureContext<T> ctx);
}

public record FailureContext<T>(
    ClaimedEvent event, T payload,
    EventOutcome outcome, Throwable cause,
    int attempts, Instant now
) { }

public sealed interface FailureDecision {
    record RetryAt(Instant when, String reason)        implements FailureDecision { }
    record Disable(String reason)                      implements FailureDecision { }
    record Delete(String reason)                       implements FailureDecision { }
}
```

### Built-in handlers

- `LogFailureHandler<T>` — decorator: logs and delegates.
- `MaxRetriesFailureHandler<T>` — decorator: on exhaustion →
  DISABLE/DELETE; otherwise delegates.
- `ExponentialBackoffFailureHandler<T>` — leaf: exp backoff + jitter + cap.
- `FixedDelayFailureHandler<T>` — leaf: fixed delay.
- `NoRetryFailureHandler<T>` — leaf: immediately DISABLE.

Listener callbacks (`onEventRetryScheduled`, `onEventDisabled`,
`onEventDeleted`) are emitted by the engine dispatcher directly after
the decision is persisted — not by a decorator in the chain. See §Q25.

### Default chain

```java
FailureHandlers.defaults() =
    new LogFailureHandler<>(
        new MaxRetriesFailureHandler<>(10, DISABLE,
            new ExponentialBackoffFailureHandler<>(
                Duration.ofSeconds(5), 2.0,
                Duration.ofHours(1), 0.2)));
```

### Per-type resolution priority

*Superseded by the 2026-08-29 amendment — the six-level order lives in
ADR-0030. `EventTypeConfig.failureHandler` never existed; the per-type
chain is held by `FailureHandlerResolver` in core.*

1. `EventHandler.failureHandler()` default method (handler-level override).
2. `EventTypeConfig.failureHandler` (bean in the Spring starter).
3. Global `FailureHandlers.defaults()`.

### YAML configuration

*Superseded by the 2026-08-29 amendment: the tree below was never
implemented; the starter binds
`event-outboxer.event-types.defaults.failure.*` /
`...overrides.<TYPE>.failure.*` instead.*

For 80% of cases the chain is built by the starter from properties:
```yaml
event-outboxer.handlers.defaults.failure:
  max-attempts: 10
  exhausted-action: DISABLE
  strategy: exponential
  base-delay: 5s
  multiplier: 2.0
  max-delay: 1h
  jitter: 0.2
  log-level: WARN
```

Per-type thin merge: `event-outboxer.handlers.types.SEND_EMAIL.failure.max-attempts:
5` overrides ONLY `max-attempts`; the remaining fields inherit from
`defaults`.

## Rationale

### Flexibility without over-engineering

Any combination of real needs is expressible as a single chain:

```java
// "fail fast" for validation handlers — the terminator returns the chain, no build()
FailureHandlers.<ValidationPayload>builder()
    .withNoRetry();

// "long retry with custom exception handling"
FailureHandlers.<BulkImportPayload>builder()
    .withLogging(Level.ERROR)                       // org.slf4j.event.Level
    .withMaxAttempts(50, ExhaustedAction.DELETE)
    .withExponentialBackoff(Duration.ofMinutes(1), 2.0, Duration.ofHours(6), 0.1);

// "different strategy for transient vs permanent" — a hand-written decorator
// (no ConditionalFailureHandler ships in builtin)
FailureHandler<HttpPayload> byCause =
    ctx -> ctx.cause() instanceof HttpTimeoutException
        ? transientHandler.onFailure(ctx)   // short retry
        : permanentHandler.onFailure(ctx);  // Disable immediately
new LogFailureHandler<>(byCause);
```

### YAML-binding in the starter (not in the core)

The core works only with pre-built `FailureHandler<T>` objects — it does not
pull in YAML / SnakeYAML. The starter transforms
`@ConfigurationProperties` into a `FailureHandlerBuilder` → chain. This
keeps plain-Java usage of the core feasible without Spring Boot.

### Retry = reschedule-to-future (not in-memory sleep)

`RetryAt(when, reason)` means "the event returns to PENDING with
`run_at = when`; the handler does NOT sleep, the worker thread is freed".
This is the only retry mechanism in MVP.

The alternative — in-memory retry via `Thread.sleep(delay)`
(resilience4j-style) — was rejected for MVP: it blocks the worker thread
(reduces throughput), breaks crash-safety (sleep does not survive a JVM
restart), complicates lease renewal. Users can wrap their code in
resilience4j for sub-100ms transient failures themselves.

### Q25: listener events are emitted by the dispatcher, not by the chain

Originally considered: embed a `NotifyListenerFailureHandler` decorator in
the default chain so every failure produces a listener event.

That design was superseded by emitting listener callbacks
(`onEventRetryScheduled`, `onEventDisabled`, `onEventDeleted`) directly
from `HandlerDispatcher`, gated on successful storage commit of the
decision. Two reasons:

1. **Post-commit semantics are stronger.** A chain-based decorator fires
   *before* the state transition is persisted — if the subsequent
   `markForRetry` / `markDisabled` fails (race with the watchdog,
   storage outage, version mismatch), the listener sees a transition
   that never happened. Dispatcher-side emission is guarded on the
   actual storage commit, so the observable sequence matches the
   persisted state exactly.
2. **No double-emission.** If both the dispatcher and the chain emit,
   every successful decision produces two listener events. Picking one
   canonical source removes the hazard entirely; `NotifyListener-
   FailureHandler` is therefore not shipped.

The observability argument from the original ADR still stands — every
failure is reported to listeners — it is simply fulfilled by the
dispatcher rather than by a chain decorator.

## Consequences

### For users

- Handlers can override the `FailureHandler` via the `failureHandler()`
  default method.
- Per-type configuration through YAML covers most cases
  (`event-outboxer.event-types.*.failure.*`, since the 2026-08-29
  amendment).
- For complex cases — a `@Bean @OutboxFailureHandler(...) FailureHandler`
  in the Spring context (ADR-0030).
- Javadoc explicitly states: "any uncaught exception becomes `Retry`
  through the FailureHandler chain".

### For maintainers

- `RetryPolicy` as a standalone interface — **removed** from all
  contracts.
- `DefaultHandlerDispatcher` calls the chain only for non-Success outcomes.
- YAML binding (building the chain) is the starter's responsibility, not
  the core's.

### Positive consequences

- Flexibility via composition.
- Testability: each handler can be unit-tested independently.
- Configuration: YAML works for basic cases; code works for complex ones.
- Observability: the listener receives every failure event automatically.

### Negative consequences

- Slightly larger API surface than `RetryPolicy`.
- Users must grasp the composition pattern (but it is not rocket science).
- Custom `FailureHandler`s must be thread-safe.

## Amendment (2026-07-26): deserialization failures go through the chain

Originally the dispatcher finalized a payload deserialization failure
straight to `DISABLED`, bypassing the chain and the attempt budget —
with no re-enable API, a single schema hiccup (typically mixed-version
replicas during a rolling deploy) was irreversible.

Deserialization failures now route through the same
`applyFailureDecision` path as handler failures, with
`FailureContext.payload = null` (the record was designed for this —
its javadoc always documented null as "failure occurred before
deserialization") and `outcome = null`. Consequences:

- A transient failure retries with the chain's backoff and heals when
  an updated replica claims the retry; attempts are consumed.
- A truly poisoned payload still ends in `DISABLED` (or `DELETE`),
  but only after `MaxRetriesFailureHandler`'s budget.
- `OutboxListener.onEventSerializationError` still fires on every
  failed deserialization, before the chain runs.

## Amendment (2026-08-29): YAML lives under event-types, precedence fixed

The `event-outboxer.handlers.defaults.failure.*` tree sketched in
§YAML configuration was never implemented — until now the starter only
accepted two beans with fixed names (`outboxDefaultFailureHandler`,
`outboxPerTypeFailureHandlers`), the samples above used a `build()`
method and a `LogLevel` type that do not exist, and a plain `@Bean
FailureHandler` was ignored without a trace.

ADR-0030 closes the gap:

- the starter binds `event-outboxer.event-types.defaults.failure.*` and
  `event-outboxer.event-types.overrides.<TYPE>.failure.*` — the same
  tree and the same thin merge as every other per-type knob — and
  builds the chain through `FailureHandlers.builder()` in
  `FailurePolicyFactory`; bad values fail startup naming the property;
- Java beans are registered through the `@OutboxFailureHandler`
  qualifier (global without a value, per type with values); the two
  legacy bean names keep working;
- §Per-type resolution priority is superseded by the six-level order
  in ADR-0030: `EventHandler.failureHandler()` → per-type bean →
  per-type YAML → global bean → YAML defaults →
  `FailureHandlers.defaults()`;
- `EventTypeConfig.failureHandler` does not exist; the per-type chain is
  held by `FailureHandlerResolver` in core, which is what
  `OutboxEngineBuilder.failureHandlerFor(...)` feeds.

The chain model, the sealed `FailureDecision`, and the "core takes
pre-built chains, the starter binds YAML" split are unchanged.

## Related decisions

- [ADR-0013](0013-outbox-listener-for-observability.md) —
  `HandlerDispatcher` is the source of listener callbacks for
  retry / disable / delete transitions (emission is gated on successful
  storage commit).
- [ADR-0015](0015-at-least-once-semantics.md) — the retry model is built on
  at-least-once.
- [ADR-0011](0011-jackson-json-only-in-mvp.md) — the evolution-friendly
  Jackson defaults that make deserialization failures rare in the first
  place.
