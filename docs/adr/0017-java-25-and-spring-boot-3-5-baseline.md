# ADR-0017: Java 25 baseline and Spring Boot 3.5.6

## Status

Accepted — **amended** (see §Amendment history).

## Date

- 2026-04-21 — original decision: Java 25 baseline.
- 2026-04-22 — amended to Java 17 baseline with JDK 21+ runtime opt-ins.
- 2026-07-27 — amended back to Java 25 baseline; Java-17 concessions
  removed.

## Context

ADR-0016 originally specified Java 17 and "Spring Boot 3" (unpinned) as
the build baseline. The first implementation pass picked Java 25 to
unlock JEP 491 (no synchronized pinning on virtual threads); a
post-review amendment lowered the baseline to Java 17 to widen the
reachable audience, keeping virtual threads as a reflection-gated
runtime opt-in.

That detour left a small, well-defined set of Java-17-compatibility
concessions in the code: a reflection helper around
`Thread.ofVirtual()` / `Executors.newThreadPerTaskExecutor(...)` and
`instanceof`-chains in place of pattern-matching `switch` on sealed
types. In July 2026 the project moved back to Java 25 as the hard
baseline and removed all of them.

## Decision

**Java 25 (LTS) is the library baseline.**

### Concrete settings

- `maven.compiler.release=25` at the parent POM level.
- `maven-enforcer-plugin` rule: `requireJavaVersion [25,)` — the build
  requires JDK 25+.
- Parent POM imports `spring-boot-dependencies:3.5.6` as a BOM — this
  remains the primary source of truth for Spring, Jackson, Micrometer,
  SLF4J, and Logback versions.
- `HandlerExecutorFactory.virtual()` calls `Thread.ofVirtual()` and
  `Executors.newThreadPerTaskExecutor(...)` directly — no reflection,
  no runtime JDK gate.
- Sealed-type routing (`EventOutcome`, `FailureDecision`) uses
  pattern-matching `switch`; exhaustiveness is compiler-enforced, so a
  new subtype fails the build instead of surfacing at runtime.
- Java 21/25 idioms are welcome throughout: unnamed variables (`_`),
  SequencedCollection (`getFirst()`/`getLast()`), `Thread.ofPlatform()`
  builders, record patterns where they read better. Preview features
  stay out.

## Rationale

### Why Java 25 as the floor

- **JEP 491 unconditional.**
  `event-outboxer.handler-executor.type=virtual` is safe with
  `synchronized`-heavy JDBC drivers on every supported runtime — no
  version caveats for users to reason about.
- **No reflection islands.** The virtual-thread factory reads like
  regular Java; misconfiguration cannot fail at bean-creation time on
  an older JVM because older JVMs are rejected up front by the
  enforcer/class-file version.
- **Compiler-enforced exhaustiveness** over sealed hierarchies in the
  dispatcher — a new `EventOutcome`/`FailureDecision` subtype is a
  compile error, not an `IllegalStateException` in production.
- **Future features are free to reach for**: scoped values, structured
  concurrency (once final), record patterns — no new opt-in machinery
  required.
- Java 25 is an LTS with wide vendor support; by mid-2026 adoption is
  broad enough that a greenfield library can require it.

### Spring Boot 3.5.6 (unchanged)

- `ContextPropagatingTaskDecorator` bundled (Spring Framework 6.1 /
  Boot 3.2+) — guarantees context propagation for handler workers (see
  ADR-0009).
- Micrometer Observation + OpenTelemetry integration stable.
- `AutoConfiguration.imports` format stable since Boot 3.0.
- Jackson default `ObjectMapper` ships with `JavaTimeModule`.
- Boot 3.x requires Java 17+ and runs happily on Java 25; downstream
  apps can override the Boot version by importing a later
  `spring-boot-dependencies` BOM ahead of ours.

## Consequences

### For users

- **Minimum runtime: Java 25.** Applications on JDK 17/21 must upgrade
  the JVM before adopting the library.
- **Virtual threads work everywhere** the library runs, pin-free
  (JEP 491), via `event-outboxer.handler-executor.type=virtual`.

### For maintainers

- maven-enforcer fails the build on JDK < 25; CI builds and releases
  on JDK 25.
- Baseline sources may use any final (non-preview) Java 25 feature.
  Preview features require an explicit ADR before adoption.

### Negative consequences

- Narrower reachable audience: Java 17/21-only deployments cannot
  consume the library. This is a deliberate trade against carrying
  reflection gates and syntax downgrades in a foundational codebase.

## Amendment history

- **2026-04-21 (original)** — decision: Java 25 baseline + pinned Boot
  3.5.6; rationale centered on JEP 491 and zero reflection.
- **2026-04-22** — decision downgraded to Java 17 baseline; JDK 21+
  virtual-thread support preserved as an opt-in runtime path via
  reflection. Motivation: widening the reachable audience while
  keeping the Java 25 benefits automatic for users on that runtime.
- **2026-07-27 (revert + modernization, current)** — baseline raised
  back to Java 25. All Java-17-compatibility concessions documented in
  the migration checklist (`docs/MIGRATION-TO-JAVA-25.md`, removed in
  the same PR as executed) were reverted, and Java 21/25 idioms
  (pattern-matching switch on sealed types, unnamed variables,
  SequencedCollection accessors, `Thread.ofPlatform()` builders) were
  adopted across modules. Rationale: the reflection gate and syntax
  downgrades carried ongoing maintenance cost, while Java 25 adoption
  had grown enough that the audience-reach benefit no longer justified
  them.

## Related decisions

- [ADR-0009](0009-spring-task-executor-in-starter.md) — depends on
  `ContextPropagatingTaskDecorator` availability.
- [ADR-0011](0011-jackson-json-only-in-mvp.md) — ObjectMapper resolution
  relies on Boot's primary `ObjectMapper` configuration.
- [ADR-0016](0016-maven-module-structure.md) — module structure; this
  ADR supersedes its Java-version reference.
