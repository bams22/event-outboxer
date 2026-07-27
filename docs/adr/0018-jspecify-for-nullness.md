# ADR-0018: JSpecify for nullness annotations

## Status

Accepted

## Date

2026-04-21

## Context

As the library grows we want API clarity about which references can be
`null` and which cannot. IDE hints, static analysis (Error Prone,
NullAway, Checker Framework, IntelliJ's dataflow), and clear
documentation all benefit from machine-readable nullness annotations.

The original question was whether to adopt JSR-305 (FindBugs)
annotations — the historical default for Java nullness — or a more
modern alternative.

## Alternatives considered

- **A. JSR-305 (`com.google.code.findbugs:jsr305`)**: widely recognized,
  but the JSR itself was never ratified, the jar has not been updated
  since 2017, and its `javax.annotation` package collides with
  `jakarta.annotation` under JPMS (split-package).
- **B. SpotBugs annotations (`com.github.spotbugs:spotbugs-annotations`)**:
  living fork of JSR-305. Still tool-specific and less broadly adopted
  than JSpecify.
- **C. Checker Framework (`org.checkerframework:checker-qual`)**:
  rigorous, great static analysis, but the annotation jar pulls tools
  configuration into consumer classpaths and the semantics are specific
  to the Checker Framework.
- **D. JetBrains annotations (`org.jetbrains:annotations`)**:
  pragmatic, excellent in IntelliJ, but not standardized and weaker
  outside the JetBrains ecosystem.
- **E. JSpecify (`org.jspecify:jspecify`)**: stable 1.0.0 released
  2024; backed by Google, JetBrains, Oracle, Meta, Microsoft, Square,
  Uber. Specifically designed as the modern successor to JSR-305.
  Single annotation-only jar, no transitive dependencies, clear
  specification of semantics (including generics). Adopted natively by
  Spring Framework 6.1+ / Spring Boot 3.3+.
- **F. No annotations**: rely on `Objects.requireNonNull` in compact
  constructors and `Optional<T>` in return types. Minimum dependency
  footprint but loses IDE/static-analysis value.

## Decision

**Option E was chosen**: JSpecify (`org.jspecify:jspecify:1.0.0`) is the
project-wide nullness-annotation library.

### Concrete conventions

- Every public package under `io.github.bams22.outboxer.*` carries a
  `package-info.java` annotated with
  `@org.jspecify.annotations.NullMarked`. Inside the package references
  are non-null by default.
- `@org.jspecify.annotations.Nullable` is added at the type-use site
  only where a value can legitimately be `null` from a consumer's
  perspective.
- The dependency is declared as `compile` scope in
  `event-outboxer-api`; it is transitive to downstream users through
  the api module.

### Scope of adoption

Immediate retrofit of `event-outboxer-api`:

- 6 `package-info.java` files (one per public api package).
- `@Nullable` added on ~18 fields / return types — `Event.claimedBy`,
  `Event.claimedAt`, `Event.lastFailReason`, `EventOutcome.Retry`
  (`delayOverride`, `cause`), `EventOutcome.Fail.cause`,
  `FailureContext` (`payload`, `outcome`, `cause`),
  `EventRetryScheduledInfo.cause`, `EventDisabledInfo.cause`,
  `EventHandler.extractLockKey(...)` return,
  `EventHandler.failureHandler()` return, `PublishOptions` (all four
  fields), `PublishRequest.options`.

Forward convention for all later modules (SPI, core, adapters,
starter): same `@NullMarked` per public package + targeted `@Nullable`
as needed.

## Rationale

- **JSR-305 is effectively dormant**. Using a never-ratified, 9-year-
  stale JSR for a new open-source library in 2026 sends the wrong
  signal and imports known JPMS problems.
- **JSpecify is the ecosystem's chosen direction**. When the library
  with the strongest backers is already the target of Spring
  Framework's own migration, alignment is clearly the safer call.
- **Minimal cost**: the jar is ~10 KB, annotation-only, with no
  transitive dependencies.
- **Immediate value**: IntelliJ 2024.1+ highlights NPE-prone code
  without any extra configuration; downstream users running NullAway or
  Error Prone get static enforcement.
- **Not locked-in**: JSpecify annotations are plain class-file
  annotations; removing them is a sed-script away if a future direction
  changes.

## Consequences

### For users

- The `event-outboxer-api` jar transitively exposes
  `org.jspecify:jspecify`. Downstream projects already using JSpecify
  (notably anything built on Spring Boot 3.3+) see no conflict; others
  gain a tiny extra jar on the classpath.
- Public contracts are clearer about what can be `null`.

### For maintainers

- Every new `.java` file MUST start in a `@NullMarked` package (create
  a `package-info.java` first when adding a new public package).
- `@Nullable` is the only qualifier we use in the positive direction —
  do NOT add redundant `@NonNull` annotations (that's the default).
- Runtime `Objects.requireNonNull(...)` checks stay where they are: the
  annotations are static guidance, not a replacement for runtime
  validation of untrusted input.
- Static enforcement via NullAway / Error Prone in CI is a post-MVP
  enhancement; for now the value is in IDE hints plus API clarity.

### Positive consequences

- Clearer contracts for callers.
- Alignment with Spring Framework and the broader modern ecosystem.
- Tool support across IntelliJ, Eclipse, Error Prone, NullAway, Checker
  Framework.

### Negative consequences

- One additional transitive dependency (annotation-only, ~10 KB).
- Requires discipline to keep annotations accurate as the code evolves.

## Related decisions

- [ADR-0017](0017-java-25-and-spring-boot-3-5-baseline.md) —
  Spring Boot 3.5.6 baseline, which uses JSpecify natively.
