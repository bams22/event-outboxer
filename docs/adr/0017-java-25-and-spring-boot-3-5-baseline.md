# ADR-0017: Java 17 baseline (with JDK 21+ opt-ins) and Spring Boot 3.5.6

## Status

Accepted — **amended** (see §Amendment history).

## Date

- 2026-04-21 — original decision: Java 25 baseline.
- 2026-04-22 — amended to Java 17 baseline with JDK 21+ runtime opt-ins.

## Context

ADR-0016 originally specified Java 17 and "Spring Boot 3" (unpinned) as
the build baseline. As we moved from architecture to implementation, the
first pass picked Java 25 as the baseline to unlock JEP 491 (no
synchronized pinning on virtual threads).

Post-review, Java 25 as a hard minimum was judged too narrow a reachable
audience for a foundational library — most enterprise Java deployments in
2026 still run 17 or 21 (LTS), with 25 adoption still ramping. Hard-gate
on Java 25 cuts off the bulk of potential users to buy a property that
most workloads do not immediately need.

## Alternatives considered (current round)

- **A. Stay on Java 25 as the hard minimum**: simplest code (direct
  Java 21+ APIs everywhere), but excludes Java 17 / 21 users.
- **B. Lower the baseline to Java 17 and drop virtual threads entirely**:
  widest audience, but loses a genuine feature for Java 21+ users.
- **C. Lower the baseline to Java 17, keep virtual threads as an opt-in
  path with runtime JDK detection**: reflective invocation of
  `Thread.ofVirtual()` and `Executors.newThreadPerTaskExecutor(...)` at
  the one call site; fails fast with a clear message on JDK < 21.

## Decision

**Option C was chosen.**

### Concrete settings

- `maven.compiler.release=17` at the parent POM level.
- `maven-enforcer-plugin` rule: `requireJavaVersion [17,)` — builds fine
  on any JDK ≥ 17.
- Parent POM imports `spring-boot-dependencies:3.5.6` as a BOM — this
  remains the primary source of truth for Spring, Jackson, Micrometer,
  SLF4J, and Logback versions.
- `HandlerExecutorFactory.virtual()` reflects into `Thread.ofVirtual()`
  and `Executors.newThreadPerTaskExecutor(...)` at bean-creation time.
  On a JDK < 21 runtime the factory throws `IllegalStateException`
  with an actionable message pointing at `Runtime.version()` and
  suggesting `type=platform` or a JVM upgrade.
- Sealed-type pattern-matching `switch` (Java 21+ syntax) is rewritten
  as `instanceof`-chain with a terminating `throw new
  IllegalStateException(...)` so runtime exhaustiveness is preserved.
- Revisit the pin when Spring Boot 4 GA ships (likely 2026H2).

## Rationale

### Why Java 17 as the floor

- **Reach.** Java 17 is the most widely deployed enterprise LTS as of
  2026 — cutting it off would halve the audience for minimal benefit.
- **Spring Boot 3.x already guarantees Java 17 minimum** — we inherit
  that floor without adding a stricter one.
- **All baseline-code features we actually use — records, sealed
  interfaces, `var`, instanceof patterns, text blocks, switch
  expressions on enums — are stable in Java 17.** The surface that
  genuinely required Java 21+ turned out to be a single executor-factory
  method plus a few pattern-matching switches.

### Why keep virtual threads as an opt-in

- `event-outboxer.handler-executor.type=virtual` is an attractive
  setting for I/O-bound handler pools on JDK 21+. Removing it entirely
  is a regression for users who have already moved to JDK 21+ (the
  majority of users who will consider this setting in the first place).
- The reflection-based gate is localized to one private method in one
  starter class — scope is narrow, maintenance burden is near zero.
- On JDK 25+ the virtual-thread path additionally benefits from JEP 491
  (no synchronized pinning on carriers) — users get this automatically
  from their runtime, no library change required.

### Why reflection over Multi-Release JAR

- MRJAR would be ideologically cleaner (native Java 21+ source in
  `META-INF/versions/21/` overriding a baseline), but for a single
  method that invokes four JDK APIs, the Maven + `maven-jar-plugin`
  multi-release configuration plus a duplicated source tree is
  disproportionate complexity.
- The reflection version is ~30 lines of well-contained code with
  clear error semantics; upgrading to MRJAR later (if the Java 21+
  surface grows) is a straightforward migration.

### Spring Boot 3.5.6 (unchanged)

- `ContextPropagatingTaskDecorator` bundled (Spring Framework 6.1 /
  Boot 3.2+) — guarantees context propagation for handler workers (see
  ADR-0009).
- Micrometer Observation + OpenTelemetry integration stable.
- `AutoConfiguration.imports` format stable since Boot 3.0.
- Jackson default `ObjectMapper` ships with `JavaTimeModule`.

## Consequences

### For users

- **Minimum runtime: Java 17.** The library runs on Java 17, 21, 25, or
  newer.
- **Virtual threads are opt-in and runtime-gated.**
  `event-outboxer.handler-executor.type=virtual` requires JDK 21+ at
  runtime. On JDK 17 the starter fails at bean creation with a clear
  message. JDK 25+ additionally gets pin-free behaviour (JEP 491)
  automatically.
- Downstream apps can override the Boot version by importing a later
  `spring-boot-dependencies` BOM ahead of ours.

### For maintainers

- CI uses JDK 25 as the build-time toolchain so the `release=17`
  constraint is enforced by javac itself (using a Java 21+ API by
  accident fails the build immediately).
- maven-enforcer fails the build on JDK < 17.
- Baseline code **must not** reference APIs introduced after Java 17
  directly. Any genuinely Java-21+-only feature should:
  1. Be invoked via reflection (`HandlerExecutorFactory.virtual()` sets
     the precedent), or
  2. Be lifted to a Multi-Release JAR if the surface grows enough that
     reflection becomes unwieldy.
- Sealed-type routing uses `instanceof`-chain with terminating
  `throw new IllegalStateException(...)`; do not reintroduce
  pattern-matching `switch`.

### Positive consequences

- **Wider reachable audience** — every Java 17+ deployment can consume
  the library.
- **Zero-config upgrade path**: Java 21 / 25 users just bump their JDK
  and get virtual threads + JEP 491 without a library release.
- Clean, reproducible versioning for all downstream consumers.
- Stable `ContextPropagatingTaskDecorator` availability.

### Negative consequences

- Baseline code loses pattern-matching `switch` on sealed types — a
  stylistic regression. Runtime exhaustiveness is preserved via
  `IllegalStateException` in the `else` branch.
- `HandlerExecutorFactory.virtual()` has one reflection island instead
  of direct calls — slightly uglier but localized.
- Users misconfiguring `type=virtual` on JDK 17 see the failure only
  at application start (bean creation time), not at compile time.

## Amendment history

- **2026-04-21 (original)** — decision: Java 25 baseline + pinned Boot
  3.5.6; rationale centered on JEP 491 and zero reflection.
- **2026-04-22 (current)** — decision downgraded to Java 17 baseline;
  JDK 21+ virtual-thread support preserved as an opt-in runtime path
  via reflection. Motivation: widening the reachable audience while
  keeping the Java 25 benefits automatic for users on that runtime.

## Related decisions

- [ADR-0009](0009-spring-task-executor-in-starter.md) — depends on
  `ContextPropagatingTaskDecorator` availability.
- [ADR-0011](0011-jackson-json-only-in-mvp.md) — ObjectMapper resolution
  relies on Boot's primary `ObjectMapper` configuration.
- [ADR-0016](0016-maven-module-structure.md) — module structure; this
  ADR supersedes its Java-version reference.
