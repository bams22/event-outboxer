# event-outboxer-bom

A versions-only Bill of Materials (`<packaging>pom</packaging>`).
Import it once and declare every `event-outboxer-*` dependency without
a version.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-bom` (type `pom`, scope `import`) |
| Manages | all 18 library modules, each at the BOM's own version |
| Manages third-party versions? | **No** — Spring, Jackson, Micrometer etc. stay under your own (or Spring Boot's) dependency management |

## Why it exists

A typical setup pulls three to six `event-outboxer-*` artifacts
(starter, storage, lock, metrics, testkit…). Mixing versions across
them is an easy way to get subtle wiring failures; the BOM makes the
version a single line. It deliberately manages *only*
`io.github.bams22` artifacts, so it can never fight with the
`spring-boot-dependencies` BOM.

## When to use it

Always — whenever you depend on any `event-outboxer-*` artifact.

## How to use it

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
    <!-- no <version> on any of these -->
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
        <artifactId>event-outboxer-testkit</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

Gradle (version catalog or platform):

```kotlin
dependencies {
    implementation(platform("io.github.bams22:event-outboxer-bom:${outboxerVersion}"))
    implementation("io.github.bams22:event-outboxer-spring-boot-starter")
    implementation("io.github.bams22:event-outboxer-storage-postgres")
    testImplementation("io.github.bams22:event-outboxer-testkit")
}
```

Notes:

- The BOM does **not** manage the `event-outboxer-spi` `tests`
  classifier artifact (used only by custom adapter authors) — declare
  its version explicitly if you consume it.
- The published BOM is flattened (`flattenMode=bom`), so every managed
  version is a literal — no property resolution needed on the
  consumer side.

## Related

- [ARTIFACTS.md](../../ARTIFACTS.md) — the pick-list of which modules to add per use case, compatibility matrix, coordinates.
