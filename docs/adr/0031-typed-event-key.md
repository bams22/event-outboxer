# ADR-0031: Typed event key — EventType<T> shared by publisher and handler

## Status

Accepted.

## Date

2026-08-29

## Context

The event type was spelled in three unlinked places: the producer's
`publish("SEND_EMAIL", dto)`, the handler's `eventType()` and its
`payloadType()`. Nothing tied them together: a typo in the string or a
wrong DTO compiled fine and surfaced only at handle time, as a
deserialization failure that — by ADR-0007's amendment — is retried
through the failure chain for hours before the event is `DISABLED`. The
handler also carried two lines of boilerplate per implementation, and
`EventOutcome.Success.INSTANCE` / `new Retry(reason, null, null)` made
the common outcomes verbose.

## Alternatives considered

- **Keep strings, validate at publish against the handler registry.**
  Couples the publisher to the handlers of *this* JVM and breaks the
  publish-only role (ADR-0029), where the handlers live elsewhere. It
  also catches the typo at runtime, not at compile time.
- **Infer the payload class from the DTO (reflection / a registry keyed
  by class).** Violates the explicitness ADR-0003 insists on, is
  ambiguous when two event types share a DTO, and still needs the string
  name somewhere.
- **A typed key `EventType<T>` shared by both sides** — chosen.

## Decision

1. `io.github.bams22.outboxer.domain.EventType<T>(String name, Class<T> payloadType)`:
   a record validated at construction (non-blank name, at most 128
   characters — the `event_type` column width), `EventType.of(name, class)`
   and the runtime escape hatch `EventType.untyped(name)`
   (`EventType<Object>`), `toString()` = name.
2. `EventHandler<T>` declares one abstract descriptor, `EventType<T> type()`;
   `eventType()` and `payloadType()` remain as defaults derived from it,
   so the engine, resolver and metrics keep reading them and existing
   framework code is untouched. Handlers declare the key once as a
   `public static final` constant and return it from `type()`.
3. `OutboxEventPublisher` is typed only: `<T> UUID publish(EventType<T>, T[, Instant | PublishOptions])`
   and `publishAll(Collection<? extends PublishRequest<?>>)` with
   `PublishRequest<T>(EventType<T>, T, PublishOptions)`. The `String`
   overloads are removed. The publisher checks
   `type.payloadType().isInstance(payload)` and rejects a mismatch with
   `PublishValidationException` — a safety net for raw types; the typed
   signature makes the mismatch a compile error.
4. Storage-side records (`PendingEvent`, `ClaimedEvent`, `Event`,
   `EventContext`) and string-keyed configuration (YAML
   `overrides.<TYPE>`, `@OutboxFailureHandler("TYPE")`,
   `failureHandlerFor(String, …)`) keep the plain name; `payload_class`
   stays a diagnostic column. The core builder and the testkit gain
   `EventType<?>` overloads that delegate to the string ones.
5. `EventOutcome` gains static factories — `success()`, `skip(reason)`,
   `retry(reason[, delay][, cause])`, `fail(reason[, cause])` — as the
   idiomatic way to produce an outcome; the records stay public for
   pattern matching.

## Rationale

One constant, two uses: the producer and the handler cannot drift, and
the compiler checks the payload class at every `publish` call. The
descriptor is still an explicit `Class<T>` — exactly what ADR-0003 asks
for, carried in one object instead of two methods. Removing the string
overloads rather than keeping them "for convenience" is deliberate:
with both available, the untyped path would stay the path of least
resistance in every example. The escape hatch exists for producers that
genuinely only know the name at runtime, and it is spelled out as such.

## Consequences

- **Users** (pre-1.0 breaking change): every handler implements
  `type()` instead of the accessor pair; every `publish` call passes the
  constant. Migration is mechanical and the compiler finds every site.
  Event names longer than 128 characters now fail at construction
  instead of at insert.
- **Maintainers**: `EventType` is API surface; new per-type
  registration methods should offer an `EventType<?>` overload.
  The engine still keys everything by `type().name()`.
- **Operations**: no storage or wire change — the stored `event_type`
  is the key's name.

## Related decisions

- [ADR-0003](0003-explicit-dto-payload.md) — explicit payload class;
  amended by this decision.
- [ADR-0012](0012-extract-lock-key-on-handler.md) — the `EventHandler`
  interface shape; amended by this decision.
- [ADR-0029](0029-publish-only-is-explicit.md) — publish-only nodes,
  the reason publish-time validation cannot consult the handler
  registry.
