# ADR-0016: Maven module structure

## Status

Accepted — amended 2026-08-29 (the starter depends on the Jackson
serializer non-optionally), 2026-09-02 (20th module:
`event-outboxer-relay-spring-cloud-stream`, ADR-0032) and 2026-09-04
(21st, unpublished module: `event-outboxer-benchmark`, ADR-0034); see
the Amendment sections at the bottom

## Date

2026-04-21

## Context

The pluggable architecture (see ADR-0010) calls for a modular layout: core,
SPI ports, adapters, starter. We need to fix the concrete set of Maven
modules, their dependencies, and the publication strategy.

## Decision

### 20 modules (+ 1 unpublished harness, see the 2026-09-04 amendment)

```
event-outboxer (root parent pom)
├── event-outboxer-bom                      BOM for consistent versions
├── event-outboxer-api                      Public contracts: interfaces, domain, exceptions
├── event-outboxer-spi                      Ports for adapters
├── event-outboxer-core                     Engine + default publisher
├── event-outboxer-storage-postgres         PG implementation of EventStore/WorkerRegistry
├── event-outboxer-storage-inmemory         Test infrastructure (ADR-0020)
├── event-outboxer-serializer-jackson       Jackson EventSerializer
├── event-outboxer-serializer-protobuf      Protobuf EventSerializer (ADR-0026)
├── event-outboxer-lock-postgres-advisory   pg_advisory_lock EntityLocker (postgres-advisory opt-out)
├── event-outboxer-lock-postgres-lease      lease-table EntityLocker — PostgreSQL default (ADR-0022)
├── event-outboxer-lock-redis               Redis/KeyDB EntityLocker
├── event-outboxer-lock-redisson            Redisson RLock EntityLocker (ADR-0036)
├── event-outboxer-cache-redis              Redis/KeyDB MetricsSnapshotCache
├── event-outboxer-metrics-micrometer       MicrometerOutboxListener
├── event-outboxer-tracing-otel             OpenTelemetry OutboxTracer (ADR-0023)
├── event-outboxer-tracing-micrometer       Micrometer Tracing OutboxTracer (ADR-0023)
├── event-outboxer-relay-spring-cloud-stream  Spring Cloud Stream relay (ADR-0032)
├── event-outboxer-admin-actuator           Actuator endpoint over OutboxAdmin
├── event-outboxer-admin-rest               REST controller over OutboxAdmin
├── event-outboxer-testkit                  Test utilities
├── event-outboxer-spring-boot-starter      Autoconfiguration + SmartLifecycle
└── event-outboxer-benchmark                Load/invariant harness — never published (ADR-0034)
```

### Coordinates

- `groupId` = `io.github.bams22`
- `artifactId` = `event-outboxer-<module>`
- Java base package = `io.github.bams22.outboxer.<module>.*`

Why `groupId=io.github.bams22` (rather than
`io.github.bams22.event-outboxer`):
- Hyphens in groupIds are technically allowed by Maven but produce visual
  oddities in IDE trees.
- Java packages cannot contain hyphens — the Maven coordinate and the
  Java namespace would diverge.
- Standard practice: db-scheduler (`com.github.kagkarlsson`/`db-scheduler-*`)
  and jobrunr (`org.jobrunr`/`jobrunr-*`) use the same pattern.

### Java packages mirror modules 1-to-1

| Module | Java package |
|---|---|
| `-api` | `io.github.bams22.outboxer.api.*`, `.domain.*`, `.domain.exception.*` |
| `-spi` | `io.github.bams22.outboxer.spi.*` |
| `-core` | `io.github.bams22.outboxer.core.*` |
| `-storage-postgres` | `io.github.bams22.outboxer.storage.postgres.*` |
| `-storage-inmemory` | `io.github.bams22.outboxer.storage.inmemory.*` |
| `-serializer-jackson` | `io.github.bams22.outboxer.serializer.jackson.*` |
| `-serializer-protobuf` | `io.github.bams22.outboxer.serializer.protobuf.*` |
| `-lock-postgres-advisory` | `io.github.bams22.outboxer.lock.postgres.advisory.*` |
| `-lock-postgres-lease` | `io.github.bams22.outboxer.lock.postgres.lease.*` |
| `-lock-redis` | `io.github.bams22.outboxer.lock.redis.*` |
| `-lock-redisson` | `io.github.bams22.outboxer.lock.redisson.*` |
| `-metrics-micrometer` | `io.github.bams22.outboxer.metrics.micrometer.*` |
| `-tracing-otel` | `io.github.bams22.outboxer.tracing.otel.*` |
| `-tracing-micrometer` | `io.github.bams22.outboxer.tracing.micrometer.*` |
| `-relay-spring-cloud-stream` | `io.github.bams22.outboxer.relay.stream.*` |
| `-testkit` | `io.github.bams22.outboxer.testkit.*` |
| `-spring-boot-starter` | `io.github.bams22.outboxer.spring.*` |

### Dependency graph

```
                   api
                    ↑
                    ├── spi ←─ storage-*, lock-*, serializer-*
                    │
                    └── core ←─ spring-boot-starter
                           ↑
                           └── testkit

     metrics-micrometer ──→ api (implements OutboxListener)
     tracing-otel, tracing-micrometer ──→ api + spi (implement OutboxTracer, ADR-0023)
```

**Invariants**:
- `core` does NOT depend on Spring (only `api` + `spi` + SLF4J).
- Adapters do NOT depend on `core`.
- `spring-boot-starter` pulls in core + the Jackson serializer (the
  default, amendment 2026-08-29); every other adapter is
  `<optional>true</optional>`.

### Starter strategy (Q34)

A single starter with one default serializer and optional
dependencies on every other adapter:

```xml
<dependencies>
    <dependency>
        <artifactId>event-outboxer-core</artifactId>
    </dependency>
    <dependency>
        <artifactId>event-outboxer-serializer-jackson</artifactId>   <!-- default serializer, non-optional -->
    </dependency>
    <dependency>
        <artifactId>event-outboxer-storage-postgres</artifactId>
        <optional>true</optional>
    </dependency>
    <!-- and similarly for lock, cache, metrics, tracing, protobuf -->

    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-autoconfigure</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-jdbc</artifactId>
    </dependency>
</dependencies>
```

Users add the starter plus the adapters they need. The autoconfig uses
`@ConditionalOnClass` to activate beans only for modules that are present.

### User's pom.xml

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.bams22</groupId>
            <artifactId>event-outboxer-bom</artifactId>
            <version>${outboxer.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.github.bams22</groupId>
        <artifactId>event-outboxer-spring-boot-starter</artifactId>
    </dependency>
    <dependency>
        <groupId>io.github.bams22</groupId>
        <artifactId>event-outboxer-storage-postgres</artifactId>
    </dependency>
    <dependency>
        <groupId>io.github.bams22</groupId>
        <artifactId>event-outboxer-lock-redis</artifactId>
    </dependency>
</dependencies>
```

### Build system

Maven, targeting **Java 25** (baseline) and Maven 3.9+. See
[ADR-0017](0017-java-25-and-spring-boot-3-5-baseline.md) for version
baseline rationale.

The root POM manages:
- Plugin versions (surefire, compiler, failsafe, maven-jar-plugin).
- Java source/target.
- Shared test dependencies (JUnit 5, AssertJ, Testcontainers, Mockito)
  via `dependencyManagement`.

The BOM POM manages:
- Versions of all `event-outboxer-*` modules.

### Testing

- Core unit tests — through `storage-inmemory` + `SettableClock`. Fast.
- Integration tests for `storage-postgres` — Testcontainers PostgreSQL 15.
- Integration tests for `lock-redis` — Testcontainers Redis 7.
- Starter smoke tests — `@SpringBootTest` + Testcontainers.

## Rationale

- **Extensibility**: a new adapter means a new module, not core changes.
- **Testability**: core unit tests do not need Testcontainers; adapter
  tests do.
- **Publication**: `io.github.*` clears Sonatype OSSRH without domain
  validation.
- **Ecosystem consistency**: naming and layout are familiar to Spring Boot
  and third-party-starter developers.

## Consequences

### For users

- Depending through the BOM gives consistent versions.
- Explicit adapter choice controls the classpath.
- The documentation includes a module compatibility matrix.

### For maintainers

- **Critical invariant**: the core does not depend on Spring. Enforced by
  the dependency graph.
- Adding a new port — in `-spi`; implementations — in separate `-*`
  modules.
- Version bumps flow through the BOM, not each module individually.
- CI matrix: JDK 25, PG 15, Redis 7 (Testcontainers).

### Positive consequences

- Clean boundaries.
- Extensible without forks.
- Standard Spring Boot starter practice.

### Negative consequences

- 20 modules — more than a monorepo. That is the price of the pluggable
  architecture. (`event-outboxer-cache-redis` was added after the
  original decision when `MetricsSnapshotCache` became an SPI port;
  `event-outboxer-lock-postgres-lease` was added by ADR-0022 so the
  lease locker ships as its own artifact, and the advisory module was
  renamed `event-outboxer-lock-postgres` →
  `event-outboxer-lock-postgres-advisory` in the same release so each
  PostgreSQL locker backend carries an explicit suffix — pre-1.0,
  no published consumers.)
- An SPI breaking change requires updates to every adapter.

## Amendment (2026-08-29): the Jackson serializer is a default starter dependency

The original Q34 sample listed `event-outboxer-serializer-jackson` as
the worked example of an optional adapter, so a Boot application with
only the starter and a storage adapter failed at startup with "no
EventSerializer beans registered" — and two of the three canonical
snippets (README quick start, the starter's minimal setup) omitted the
module. That contradicts how Spring Boot starters behave: a starter
ships a sensible default and lets you swap it. For the one alternative
format we ship (ADR-0026) the answer was "add a module and set one
property" anyway.

The starter now declares `event-outboxer-serializer-jackson` as a
regular (non-optional) dependency:

- JSON via Jackson is the zero-config write serializer; the ADR-0025
  resolution rules are unchanged (still the bean named
  `outboxEventSerializer`, still overridable, still read-only once
  `event-outboxer.serializer.write-format` picks another format).
- Protobuf users keep Jackson on the classpath and set
  `write-format=protobuf` (Jackson stays registered for reads — the
  migration-safe path), or exclude
  `event-outboxer-serializer-jackson` from the starter in their pom
  for a protobuf-only classpath (ADR-0025 rule 2 then applies).
- Jackson (`jackson-databind` + `jsr310` / `jdk8` /
  `parameter-names`, versions from the Spring Boot BOM) becomes a
  transitive dependency of every starter consumer; in practice every
  Boot web application already has it.
- Excluding the module without registering another serializer is the
  only way left to reach the "no serializer" failure; the starter
  raises `NoEventSerializersException` and maps it to a
  `FailureAnalyzer` diagnosis (`OutboxSerializerFailureAnalyzer`)
  naming the three ways out.

Storage, lock, cache, metrics and tracing adapters remain optional —
there is no sensible default storage (ADR-0020), and the rest are
opt-in features.

## Amendment (2026-09-02): 20th module — the Spring Cloud Stream relay

ADR-0032 adds `event-outboxer-relay-spring-cloud-stream` (package
`io.github.bams22.outboxer.relay.stream`): a facade + built-in
`EventHandler` that relays outbox events to a broker through
`StreamBridge`. Like the admin modules it is a self-wiring Spring
surface — its own `AutoConfiguration.imports`, no starter involvement,
depends on `-api` (plus `-serializer-jackson`, since its envelope
requires the `jackson-json` write format). The module tree, package
table and module counts above include it. The parent pom now also
imports `spring-cloud-dependencies` (after `spring-boot-dependencies`,
so Boot wins on overlaps) to manage the module's Spring Cloud Stream
dependency.

## Amendment (2026-09-04): an unpublished 21st module — the benchmark harness

ADR-0034 adds `event-outboxer-benchmark` (package
`io.github.bams22.outboxer.benchmark`) to the reactor so that it
always compiles and unit-tests against the working tree, while
excluding it from everything a consumer could see: `maven.deploy.skip`,
`skipPublishing`, `gpg.skip`, `maven.javadoc.skip`, `maven.source.skip`
and `japicmp.skip` are `true` in its pom, and it is listed neither in
the BOM nor in `ARTIFACTS.md`. It is the first module allowed to depend
on the starter (it boots the library as deployed) and on Testcontainers
at compile scope (it starts a disposable PostgreSQL when none is
given). The "20 modules" count above refers to published artifacts and
stays; the reactor now builds 21 plus the relocation stub. The `bench`
Maven profile repackages the module into an executable jar.

## Related decisions

- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — the pluggable
  architecture is the root principle.
- [ADR-0002](0002-participate-in-client-transaction.md) — Spring
  integration lives only in the starter.
- [ADR-0009](0009-spring-task-executor-in-starter.md) — Spring classes
  live only in the starter.
