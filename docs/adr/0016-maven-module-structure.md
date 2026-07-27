# ADR-0016: Maven module structure

## Status

Accepted

## Date

2026-04-21

## Context

The pluggable architecture (see ADR-0010) calls for a modular layout: core,
SPI ports, adapters, starter. We need to fix the concrete set of Maven
modules, their dependencies, and the publication strategy.

## Decision

### 16 modules

```
event-outboxer (root parent pom)
├── event-outboxer-bom                      BOM for consistent versions
├── event-outboxer-api                      Public contracts: interfaces, domain, exceptions
├── event-outboxer-spi                      Ports for adapters
├── event-outboxer-core                     Engine + default publisher
├── event-outboxer-storage-postgres         PG implementation of EventStore/WorkerRegistry
├── event-outboxer-storage-inmemory         Test infrastructure (ADR-0020)
├── event-outboxer-serializer-jackson       Jackson EventSerializer
├── event-outboxer-lock-postgres-advisory   pg_advisory_lock EntityLocker (postgres-advisory opt-out)
├── event-outboxer-lock-postgres-lease      lease-table EntityLocker — PostgreSQL default (ADR-0022)
├── event-outboxer-lock-redis               Redis/KeyDB EntityLocker
├── event-outboxer-cache-redis              Redis/KeyDB MetricsSnapshotCache
├── event-outboxer-metrics-micrometer       MicrometerOutboxListener
├── event-outboxer-admin-actuator           Actuator endpoint over OutboxAdmin
├── event-outboxer-admin-rest               REST controller over OutboxAdmin
├── event-outboxer-testkit                  Test utilities
└── event-outboxer-spring-boot-starter      Autoconfiguration + SmartLifecycle
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
| `-lock-postgres-advisory` | `io.github.bams22.outboxer.lock.postgres.advisory.*` |
| `-lock-postgres-lease` | `io.github.bams22.outboxer.lock.postgres.lease.*` |
| `-lock-redis` | `io.github.bams22.outboxer.lock.redis.*` |
| `-metrics-micrometer` | `io.github.bams22.outboxer.metrics.micrometer.*` |
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
```

**Invariants**:
- `core` does NOT depend on Spring (only `api` + `spi` + SLF4J).
- Adapters do NOT depend on `core`.
- `spring-boot-starter` pulls in core + optional adapters via
  `<optional>true</optional>`.

### Starter strategy (Q34)

A single starter with optional dependencies on adapters:

```xml
<dependencies>
    <dependency>
        <artifactId>event-outboxer-core</artifactId>
    </dependency>
    <dependency>
        <artifactId>event-outboxer-serializer-jackson</artifactId>
        <optional>true</optional>
    </dependency>
    <!-- and similarly for storage, lock, metrics -->

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

Maven, targeting **Java 17** (baseline) and Maven 3.9+. See
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
- CI matrix: JDK 17, PG 15, Redis 7 (Testcontainers).

### Positive consequences

- Clean boundaries.
- Extensible without forks.
- Standard Spring Boot starter practice.

### Negative consequences

- 16 modules — more than a monorepo. That is the price of the pluggable
  architecture. (`event-outboxer-cache-redis` was added after the
  original decision when `MetricsSnapshotCache` became an SPI port;
  `event-outboxer-lock-postgres-lease` was added by ADR-0022 so the
  lease locker ships as its own artifact, and the advisory module was
  renamed `event-outboxer-lock-postgres` →
  `event-outboxer-lock-postgres-advisory` in the same release so each
  PostgreSQL locker backend carries an explicit suffix — pre-1.0,
  no published consumers.)
- An SPI breaking change requires updates to every adapter.

## Related decisions

- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — the pluggable
  architecture is the root principle.
- [ADR-0002](0002-participate-in-client-transaction.md) — Spring
  integration lives only in the starter.
- [ADR-0009](0009-spring-task-executor-in-starter.md) — Spring classes
  live only in the starter.
