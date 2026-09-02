# ADR-0030: Failure policy: YAML thin merge and the @OutboxFailureHandler qualifier

## Status

Accepted.

## Date

2026-08-29

## Context

The retry/backoff policy is the knob operators touch most, yet the
starter only accepted it as Java beans with fixed names
(`outboxDefaultFailureHandler`, a `Map<String, FailureHandler<?>>`
named `outboxPerTypeFailureHandlers`). ADR-0007 had promised a YAML
tree (`event-outboxer.handlers.defaults.failure.*`) that was never
implemented; the documentation showed a bare `@Bean
FailureHandler<SendEmailPayload>` which the starter silently ignored,
and its samples used a `build()` method and a `LogLevel` type that do
not exist. Nothing warned when a bean did not reach the engine.

## Alternatives considered

- **Implement the `handlers.*` tree as sketched in ADR-0007.** A second
  per-type tree next to `event-types.overrides.<TYPE>` — two places to
  look for one event type's settings.
- **Pick `FailureHandler<T>` beans by payload type (`ResolvableType`).**
  Invisible magic, ambiguous when two event types share a DTO, no
  place for a global chain, and event types are strings, not classes.
- **Value-carrying qualifier + the existing `event-types` tree** —
  chosen.

## Decision

1. **YAML** — `event-outboxer.event-types.defaults.failure.*` and
   `event-outboxer.event-types.overrides.<TYPE>.failure.*` with the
   keys `strategy` (`exponential` / `fixed` / `none`), `max-attempts`,
   `exhausted-action` (`DISABLE` / `DELETE`), `base-delay`,
   `multiplier`, `max-delay`, `jitter`, `fixed-delay`, `log-level`
   (`OFF` removes the logging decorator). The same thin merge as the
   other per-type knobs: override → `defaults.failure` → the library
   chain `FailureHandlers.defaults()`. `FailurePolicyFactory` (starter)
   validates every key on the layer that sets it and the cross-field
   rule (`max-delay >= base-delay`) on the merged policy, naming the
   exact property in the error; it builds the chain through
   `FailureHandlers.builder()`, so YAML and Java produce identical
   decorators.
2. **Java** — the qualifier `@OutboxFailureHandler` on a
   `FailureHandler` bean: no value = the global chain, one or more
   event types = the chain for those types. The legacy bean names keep
   working and count as claims on the same slots.
3. **Precedence** — the most specific source wins; Java beats YAML at
   equal specificity: `EventHandler.failureHandler()` → per-type bean
   → per-type YAML → global bean → YAML defaults → library chain. A
   per-type YAML override always layers on YAML defaults, never on a
   global Java bean (which is opaque to the merge).
4. **Conflicts fail fast** — two claims on one slot raise
   `AmbiguousOutboxFailureHandlerException`, rendered by
   `OutboxFailureHandlerFailureAnalyzer` with the one-bean-per-slot rule
   and the YAML alternative.
5. **Nothing is ignored silently** — `FailureHandler` beans with
   neither the annotation nor a legacy name are listed in a startup
   WARN that also names the legitimate false positive (a bean returned
   from `EventHandler.failureHandler()`).

## Rationale

One tree per event type: pool size, poll interval and retry policy of
`SEND_EMAIL` sit under one key. The qualifier mirrors ADR-0024
(`@OutboxDataSource`) and ADR-0027 (`@OutboxRedisConnection`) — intent
stated on the bean, discoverable from the annotation's Javadoc, no
string names to remember. Keeping the legacy names avoids a needless
break for the one existing test that uses them, at no design cost.
Building YAML chains through the public builder keeps the core
Spring-free (ADR-0007 §YAML-binding) and guarantees the two paths never
drift.

## Consequences

- **Users**: retries become a deployment concern (`application-prod.yml`
  can shorten `max-attempts` for a type without a code change); Java
  stays available for anything the builder cannot express. Existing
  `outboxDefaultFailureHandler` / `outboxPerTypeFailureHandlers` beans
  keep working. A bean that also carries a conflicting claim now fails
  startup instead of one claim silently winning.
- **Maintainers**: `FailurePolicyFactory.FailurePolicy.LIBRARY_DEFAULTS`
  must track `FailureHandlers.defaults()`; new chain knobs are added in
  both. Core is untouched (`FailureHandlerResolver`, `OutboxEngineBuilder`).
- **Operations**: the effective policy per type is readable from
  configuration metadata / `/actuator/configprops`; a WARN on startup
  lists unused `FailureHandler` beans.

## Related decisions

- [ADR-0007](0007-failure-handler-chain-of-responsibility.md) — the
  chain model; amended by this decision (YAML tree, precedence).
- [ADR-0024](0024-outbox-datasource-selection.md) — the qualifier
  pattern this one mirrors.
- [ADR-0027](0027-starter-managed-redis-connection.md) — same pattern
  for the Redis connection.
