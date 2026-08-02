# event-outboxer-serializer-jackson

The default payload serializer: Jackson JSON, format id
**`jackson-json`**, text lane. Payloads land in the `payload JSONB`
column — human-readable in psql and GIN-indexable.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-serializer-jackson` |
| Java package | `io.github.bams22.outboxer.serializer.jackson` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `jackson-databind` + `jsr310`/`jdk8`/`parameter-names` modules |
| Format id | `jackson-json` (stamped into `payload_format`, never renamed — [ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md)) |

## Why it exists

[ADR-0011](../adr/0011-jackson-json-only-in-mvp.md): JSON via Jackson
is the MVP wire format — ubiquitous, debuggable, evolution-friendly.
The serializer is a thin `EventSerializer` adapter so the engine stays
format-agnostic and other formats
([protobuf](event-outboxer-serializer-protobuf.md), custom) can
coexist, routed per event by the stored `payload_format`.

## What it does

- **`JacksonEventSerializer`** — `EventSerializer` around a
  caller-supplied `ObjectMapper` (it never builds one internally, so
  Spring apps can share Boot's mapper). `serialize` →
  `SerializedPayload.ofText(json)`; failures wrap into
  `PublishSerializationException` (OUTBOX-102) /
  `PayloadDeserializationException` (OUTBOX-202). Stateless and
  thread-safe; one instance serves all event types.
- **`JacksonObjectMapperFactory.defaults()`** — the canonical outbox
  mapper (a fresh instance per call, extend freely):
  `JavaTimeModule` + `Jdk8Module` + `ParameterNamesModule`,
  ISO-8601 dates (`WRITE_DATES_AS_TIMESTAMPS` off,
  `WRITE_DATES_WITH_ZONE_ID` on), and — deliberately —
  `FAIL_ON_UNKNOWN_PROPERTIES` **disabled**: during a rolling deploy,
  mixed-version replicas must read each other's payloads; strictness
  would disable events instead of processing them. See
  [CONFIGURATION.md §DTO evolution](../CONFIGURATION.md#dto-evolution-and-rolling-deploys)
  for the resulting compatibility rules (add/remove fields safe,
  rename via `@JsonAlias`, never change a type).

Text-lane consequence: PostgreSQL stores `JSONB`, so round-trips are
*semantic*, not byte-identical (whitespace and key order are
canonicalized).

## When to use it

- **Default choice** for almost every setup: readable payloads,
  effortless DTO evolution, zero schema tooling.
- Reach for [protobuf](event-outboxer-serializer-protobuf.md) instead
  when payloads are large/hot enough that JSON size and parse cost
  matter, or when you already maintain `.proto` schemas.
- Both can coexist — reads always route by the stored format, and
  `write-format-per-type` moves individual event types
  ([ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md)).

## How to configure it

### With Spring Boot

Add the module (the starter treats it as optional) — no properties
needed; `JacksonSerializerAutoConfiguration` activates on classpath
presence and registers the bean **`outboxEventSerializer`**:

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-serializer-jackson</artifactId>
</dependency>
```

The `ObjectMapper` resolves in this order:

1. a bean named/qualified **`outboxObjectMapper`** — the explicit
   override for outbox-specific tuning:

   ```java
   @Bean
   public ObjectMapper outboxObjectMapper() {
       return JsonMapper.builder()
           .addModule(new JavaTimeModule())
           .build();
   }
   ```

2. Spring Boot's primary `ObjectMapper` (your app-wide configuration);
3. `JacksonObjectMapperFactory.defaults()`.

The bean name `outboxEventSerializer` is load-bearing: with several
serializer beans registered it is the tiebreaker that keeps Jackson
the **write** serializer unless you set
`event-outboxer.serializer.write-format` (see
[CONFIGURATION.md §Serialization](../CONFIGURATION.md#serialization)).
Declaring your own bean under that name replaces the auto-configured
one entirely.

### Without Spring

```java
EventSerializer serializer =
    new JacksonEventSerializer(JacksonObjectMapperFactory.defaults());

OutboxEngine engine = new OutboxEngineBuilder()
    .eventSerializer(serializer)
    // ...
    .build();
```

## Related

- [CONFIGURATION.md §Serialization](../CONFIGURATION.md#serialization) — write-format resolution, per-type overrides, format migration recipe.
- ADRs: [0003](../adr/0003-explicit-dto-payload.md) (explicit DTOs), [0011](../adr/0011-jackson-json-only-in-mvp.md), [0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md).
