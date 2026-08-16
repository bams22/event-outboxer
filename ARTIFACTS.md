# Artifacts

All artifacts publish to Maven Central under `groupId = io.github.bams22`.
Detailed per-module documentation (purpose, when to use, configuration)
lives in [docs/modules/](docs/modules/README.md).

## Pick-list

The table below is the quickest way to decide what to add to your `pom.xml`.

| Goal | Modules to add | Transitive runtime cost |
|---|---|---|
| Spring Boot + PostgreSQL (typical production) | `event-outboxer-spring-boot-starter` <br> `event-outboxer-storage-postgres` <br> `event-outboxer-lock-postgres-lease` <br> `event-outboxer-metrics-micrometer` (optional) | Spring Boot 3.5, PostgreSQL JDBC, HikariCP (via your `spring-boot-starter-jdbc`), Micrometer. |
| Spring Boot + PG with Redis-coordinated locks | `event-outboxer-spring-boot-starter` <br> `event-outboxer-storage-postgres` <br> `event-outboxer-lock-redis` | Additional: Lettuce 6. |
| Plain Java, no Spring | `event-outboxer-core` <br> `event-outboxer-storage-postgres` (or inmemory) <br> `event-outboxer-serializer-jackson` (or `-serializer-protobuf`) <br> `event-outboxer-lock-postgres-lease` (or postgres-advisory / redis / noop) | SLF4J, Jackson (or protobuf-java), adapter dependencies. |
| Unit / integration tests for your handlers | `event-outboxer-testkit` (test scope) | Transitively brings in-memory adapter + Jackson. |

Always import the BOM first and let it manage versions:

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
```

## Module matrix

| Module | Description | Depends on (compile) | When to use |
|---|---|---|---|
| `event-outboxer-bom` | Versions-only BOM. | — | Always — controls `event-outboxer-*` versions in one place. |
| `event-outboxer-api` | Publisher, handler, listener, domain, exceptions. | `slf4j-api`, `jspecify`. | Transitive; you rarely add this directly. |
| `event-outboxer-spi` | Ports (`EventStore`, `WorkerRegistry`, `EntityLocker`, `EventSerializer`, `Clock`, `ConnectionSupplier`). | `event-outboxer-api`. | Transitive. Consumers who build custom adapters depend on it directly. |
| `event-outboxer-core` | Engine: poller, dispatcher, in-flight registry, maintenance tasks, default publisher. **Spring-free.** | `event-outboxer-api`, `event-outboxer-spi`. | Transitive when using the starter. Plain-Java users add it directly. |
| `event-outboxer-storage-inmemory` | Thread-safe in-process `EventStore` / `WorkerRegistry` / `EntityLocker`. | `event-outboxer-api`, `event-outboxer-spi`. | Tests, dev setups, experimentation. Do NOT use in production. |
| `event-outboxer-storage-postgres` | PG 15+ backend with CTE + `SKIP LOCKED` claim, optional archive. Ships Flyway migrations. | `event-outboxer-api`, `event-outboxer-spi`, `postgresql` JDBC, `flyway-core` (optional). | Production default. |
| `event-outboxer-serializer-jackson` | `JacksonEventSerializer` + `JacksonObjectMapperFactory.defaults()`. | Jackson databind + JavaTime + Jdk8 + ParameterNames. | Transitive via the starter; add directly in plain-Java setups. |
| `event-outboxer-serializer-protobuf` | `ProtobufEventSerializer` (format `protobuf`, bytes lane; payloads are protoc-generated `Message` classes, ADR-0026). | `protobuf-java`. | Opt-in: add next to the starter and set `write-format=protobuf`, or as the sole serializer in protobuf-only setups. |
| `event-outboxer-lock-postgres-lease` | Lease-table `EntityLocker` (`entity_locks`, ADR-0022) — no pinned connections, pgBouncer-safe, TTL honoured. Ships migration V005. | PostgreSQL JDBC. | Recommended PostgreSQL locker (`lock.type=postgres-lease`). |
| `event-outboxer-lock-postgres-advisory` | `pg_advisory_lock`-backed `EntityLocker` (session-scoped; pins one pooled connection per held lock, incompatible with pgBouncer transaction pooling). | PostgreSQL JDBC. | Opt-out (`lock.type=postgres-advisory`) for immediate clean-crash release. |
| `event-outboxer-lock-redis` | Redis/KeyDB `EntityLocker` with fencing-token unlock. | Lettuce 6. | Multi-region or cross-DB deployments. |
| `event-outboxer-cache-redis` | Redis/KeyDB `MetricsSnapshotCache` — shares the metrics snapshot across replicas. | Lettuce 6. | Fleets where per-JVM snapshot queries would hammer the DB. |
| `event-outboxer-metrics-micrometer` | `OutboxListener` publishing to a Micrometer `MeterRegistry`. | `micrometer-core`. | Any Boot app with Micrometer/Observation; the starter auto-wires it if present. |
| `event-outboxer-tracing-otel` | OpenTelemetry `OutboxTracer` — publish→handle trace continuity (ADR-0023); works with the OTel Java agent. | `opentelemetry-api`. | OTel-instrumented apps without Boot's Micrometer Tracing bridge; auto-detected by the starter. |
| `event-outboxer-tracing-micrometer` | Micrometer `OutboxTracer` on the Observation API (ADR-0023) — propagation follows `management.tracing.*`; side effect: four meters under `<prefix>.publish{,.active}` / `<prefix>.process{,.active}`. | `micrometer-observation`, `micrometer-tracing`. | Boot Actuator tracing setups; wins over the OTel adapter when both are present. |
| `event-outboxer-admin-actuator` | Actuator endpoint (`outboxadmin`) over the `OutboxAdmin` SPI. | Spring Boot Actuator. | Ops surface via the management port (ADR-0019). |
| `event-outboxer-admin-rest` | Opt-in REST controller over `OutboxAdmin` with configurable `@PreAuthorize` authority. | Spring Web (+ optional Spring Security). | Ops surface on the app port when Actuator is not exposed (ADR-0019). |
| `event-outboxer-testkit` | `SettableClock`, `ManualEngine`, `OutboxTestContext`, `RecordingOutboxListener`, fluent assertions, JUnit 5 extension. | `event-outboxer-core`, in-memory adapter, Jackson serializer. | Test-scope dependency for handler tests. |
| `event-outboxer-spring-boot-starter` | Auto-configuration, property binding, `SmartLifecycle`, `TransactionAwareDataSourceProxy` wiring, actuator health. | Spring Boot auto-configure, jdbc, validation, Actuator (optional), every adapter (optional). | Any Spring Boot 3.5+ app. |

## Compatibility

- **Java**: baseline **JDK 25** (LTS). Virtual threads
  (`event-outboxer.handler-executor.type=virtual`) work natively and are
  pin-free with `synchronized`-heavy JDBC drivers (JEP 491). See ADR-0017.
- **Maven**: requires **3.9+**. The project ships a Maven Wrapper pinned
  to 3.9.12 (`./mvnw`).
- **Spring Boot**: built against **3.5.6** via the
  `spring-boot-dependencies` BOM. Works with newer 3.x minors if
  Micrometer / Jackson / Spring Framework stay compatible; drop the BOM
  import in your own pom to override.
- **PostgreSQL**: **15+** for the storage adapter (partial indexes,
  JSONB, CTE-in-UPDATE). Earlier versions will not apply `V001`.
- **Redis / KeyDB**: Redis **7+** or KeyDB **6+** for the Redis locker.
- **Lettuce**: any 6.x via Spring Boot's managed version.

## Artifacts per release

Each module publishes the standard three-jar set plus signatures:

| File | Purpose |
|---|---|
| `${artifactId}-${version}.jar` | Main compiled classes. |
| `${artifactId}-${version}-sources.jar` | Source attachment for IDE drill-down. |
| `${artifactId}-${version}-javadoc.jar` | Generated Javadoc. |
| `*.pom` | Dependency metadata. |
| `*.asc` | Detached GPG signature per artifact and pom. |
| `*.md5` / `*.sha1` | Checksums (generated by the deploy plugin). |

The `event-outboxer-spi` jar is also published with the `tests` classifier
so adapter modules can extend the abstract contract tests.

## Coordinates cheat-sheet

```
io.github.bams22:event-outboxer-bom:0.2.0                  (pom)
io.github.bams22:event-outboxer-api:0.2.0
io.github.bams22:event-outboxer-spi:0.2.0
io.github.bams22:event-outboxer-spi:0.2.0:tests            (classifier)
io.github.bams22:event-outboxer-core:0.2.0
io.github.bams22:event-outboxer-storage-inmemory:0.2.0
io.github.bams22:event-outboxer-storage-postgres:0.2.0
io.github.bams22:event-outboxer-serializer-jackson:0.2.0
io.github.bams22:event-outboxer-serializer-protobuf:0.3.0     (ships in 0.3.0)
io.github.bams22:event-outboxer-lock-postgres-lease:0.3.0     (ships in 0.3.0)
io.github.bams22:event-outboxer-lock-postgres-advisory:0.3.0  (0.2.0 shipped as event-outboxer-lock-postgres)
io.github.bams22:event-outboxer-lock-redis:0.2.0
io.github.bams22:event-outboxer-cache-redis:0.3.0          (ships in 0.3.0)
io.github.bams22:event-outboxer-metrics-micrometer:0.2.0
io.github.bams22:event-outboxer-tracing-otel:0.3.0         (ships in 0.3.0)
io.github.bams22:event-outboxer-tracing-micrometer:0.3.0   (ships in 0.3.0)
io.github.bams22:event-outboxer-admin-actuator:0.3.0       (ships in 0.3.0)
io.github.bams22:event-outboxer-admin-rest:0.3.0           (ships in 0.3.0)
io.github.bams22:event-outboxer-testkit:0.2.0
io.github.bams22:event-outboxer-spring-boot-starter:0.2.0
```
