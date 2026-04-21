# ADR-0017: Java 25 and Spring Boot 3.5.6 as the baseline

## Status

Accepted

## Date

2026-04-21

## Context

ADR-0016 originally specified Java 17 and "Spring Boot 3" (unpinned) as
the build baseline. As we move from architecture to implementation, these
choices need to be concrete:

- Which Java LTS do we target?
- Do we pin an exact Spring Boot version, or accept a range?
- What constraints do downstream users inherit from our choice?

## Alternatives considered

- **A. Java 17 + Spring Boot 3.x (unpinned)**: broadest reach; virtual
  threads are first-class but `synchronized` still pins the carrier
  thread (fixed only in JDK 24+).
- **B. Java 21 (LTS) + Spring Boot 3.5.6**: previous LTS, widely
  deployed. Virtual threads stable, but synchronized pinning in JDBC
  drivers remains a concern.
- **C. Java 25 (current LTS) + Spring Boot 3.5.6 pinned**: newest LTS,
  **JEP 491 eliminates virtual-thread pinning on `synchronized`**,
  making VT + JDBC combinations production-safe; faster GC (ZGC
  generational by default); stable language features.

## Decision

**Option C was chosen**: Java 25 (LTS) + Spring Boot 3.5.6 pinned via
`<dependencyManagement>` BOM import.

### Concrete settings

- `maven.compiler.release=25` at the parent POM level.
- `maven-enforcer-plugin` rule: `requireJavaVersion 25`.
- Parent POM imports `spring-boot-dependencies:3.5.6` as a BOM — this
  becomes the primary source of truth for Spring, Jackson, Micrometer,
  SLF4J, and Logback versions.
- Revisit the pin when Spring Boot 4 GA ships (likely 2026H2).

## Rationale

### Java 25 benefits

- **JEP 491 (Synchronize Virtual Threads without Pinning)** — delivered
  in JDK 24, present in JDK 25. This removes the pinning concern for
  JDBC drivers and other `synchronized`-heavy code running on virtual
  threads. It makes `outbox.handler-executor.type=virtual` a production-
  grade option for I/O-bound handlers, not just a toy.
- **Virtual threads are first-class** (stable since 21; now with
  improved carrier behavior).
- **Pattern matching for switch, records, sealed types, text blocks**
  — all stable.
- **Generational ZGC** — default since JDK 23; very low pause times
  suit a poller-plus-executor workload well.
- **Current LTS**: released September 2025; years of security patch
  support ahead.

### Spring Boot 3.5.6 benefits

- `ContextPropagatingTaskDecorator` is bundled (Spring Framework 6.1 /
  Boot 3.2+) — guarantees context propagation for handler workers (see
  ADR-0009).
- Micrometer Observation + OpenTelemetry integration stable.
- `AutoConfiguration.imports` format stable since Boot 3.0.
- Jackson default `ObjectMapper` ships with `JavaTimeModule` — our
  resolution strategy (ADR-0011 amendments) can safely rely on it.
- Boot 3.5 ships Java-17-baseline bytecode, so the jar runs on any JVM
  from 17 to 25+. Our code, however, is compiled at `release=25` and
  therefore requires 25 at runtime — Boot's broader bytecode floor does
  not weaken our own floor.

### Pinning the Boot version

Unpinned `spring-boot-dependencies` would let transitive resolution drift
between consumers, yielding unpredictable behavior on e.g. bean
conditions, config property binding, autoconfiguration ordering. Pinning
to a single well-tested patch version is safer for a library. Downstream
apps still override by importing a newer `spring-boot-dependencies` in
their own `<dependencyManagement>` **before** ours — that precedence is
standard Maven behavior.

### Trade-offs accepted

- Users on Java 21 (the previous LTS, still supported until 2029) cannot
  run the library as-published. They can fork and rebuild with
  `release=21` for their environment — but the upstream baseline is 25.
  We expect Java 25 adoption to be rapid because it is a current LTS
  with major VT improvements.
- Users on older Boot 3.0–3.4 must upgrade to 3.5.x or newer. Spring
  Boot 3.5 is a standard maintenance line; upgrades from 3.x to 3.y are
  typically low-risk.

## Consequences

### For users

- Minimum runtime: Java 25, Spring Boot 3.5.6+.
- Virtual threads are a one-property opt-in without JDK preview flags
  and with no synchronized-pinning penalty.
- Downstream apps can override the Boot version by importing a later
  `spring-boot-dependencies` BOM ahead of ours.

### For maintainers

- CI uses JDK 25 as the single primary target. JDK 29 (next LTS) will
  join the matrix when released.
- Parent POM imports `spring-boot-dependencies:3.5.6` once; no module
  needs to declare Spring versions.
- maven-enforcer fails the build on JDK < 25.
- Any Boot version bump is an ADR-worthy change — add a new ADR or
  amend this one.
- Supersedes ADR-0016's "Java 17" references. ADR-0016 has been amended
  to link here.

### Positive consequences

- Clean, reproducible versioning for all downstream consumers.
- Modern JVM features (virtual threads without pinning, generational
  ZGC) usable without flags.
- Stable `ContextPropagatingTaskDecorator` availability.
- Eliminates the VT-pinning risk item from the implementation plan.

### Negative consequences

- Excludes users on Java 17 / 21 and Boot 3.0–3.4.
- Library is coupled to a specific Boot line; upgrades require a library
  release.

## Related decisions

- [ADR-0009](0009-spring-task-executor-in-starter.md) — depends on
  `ContextPropagatingTaskDecorator` availability.
- [ADR-0011](0011-jackson-json-only-in-mvp.md) — ObjectMapper resolution
  relies on Boot's primary `ObjectMapper` configuration.
- [ADR-0016](0016-maven-module-structure.md) — module structure; this
  ADR supersedes its Java-version reference.
