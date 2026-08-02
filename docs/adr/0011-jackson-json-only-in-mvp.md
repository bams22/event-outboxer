# ADR-0011: Jackson JSON as the only serializer in MVP

## Status

Accepted — amended 2026-07-26 (shipped Jackson defaults reconciled
with this ADR) and 2026-08-02 (SPI shape and schema clauses superseded
by [ADR-0025](0025-binary-capable-serializer-spi-and-payload-format.md);
see the Amendment sections at the bottom)

## Date

2026-04-20

## Context

The event payload serialization format is an architectural decision.
Options: Jackson JSON, Jackson Smile/CBOR, Protobuf, Avro, Kryo, Java
Native, Apache Fury.

Initially we proposed a pluggable architecture with per-type serializer
choice, a `payload_format` column in the DB, and a fallback chain for
format migration.

After discussion it was recognized that JSON covers every current need.
YAGNI wins over a complex multi-serializer architecture.

## Alternatives considered

- **A. Pluggable with per-type override + payload_format column**: full
  flexibility, but complex and unneeded for MVP.
- **B. Pluggable through SPI, single implementation (Jackson) in MVP**: the
  architecture is ready to expand without paying unnecessary complexity
  today.
- **C. Jackson hard-coded in the core**: maximum simplification, at the
  price of losing replaceability.

## Decision

**Option B was chosen**.

### Minimal SPI port

```java
public interface EventSerializer {
    String serialize(Object payload);
    <T> T deserialize(String data, Class<T> type);
}
```

No `format()`, `supports()`, `byte[]` signatures. If a binary format is
needed later, we extend the SPI (a breaking change is allowed in MVP).

### Implementation — `event-outboxer-serializer-jackson`

A separate module depending on `api` + `spi` + `jackson-databind`.

`JacksonEventSerializer` takes any pre-built `ObjectMapper` via its
constructor — the adapter itself does NOT build one. This keeps it
decoupled from Spring DI and usable in plain-Java setups.

`JacksonObjectMapperFactory.defaults()` is a static factory in the same
module that produces the library's default `ObjectMapper`:

```java
public static ObjectMapper defaults() {
    return JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .addModule(new Jdk8Module())
        .addModule(new ParameterNamesModule())
        .enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID)
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
        .build();
}
```

### ObjectMapper resolution in the Spring Boot starter

Priority order (documented contract):

1. **`@Qualifier("outboxObjectMapper") ObjectMapper`** bean if present —
   wins. This is the explicit opt-in: the user wants a dedicated mapper
   different from the application's primary one.
2. **Primary `ObjectMapper`** from the Spring context (Boot's default)
   — used when no qualified bean exists. Boot's default already
   registers `JavaTimeModule`, so records carrying `Instant` work out of
   the box.
3. **`JacksonObjectMapperFactory.defaults()`** fallback — only reached
   in Spring setups where no `ObjectMapper` bean exists (unusual).

Implemented in `JacksonSerializerAutoConfiguration`:

```java
@Bean
@ConditionalOnMissingBean(name = "outboxEventSerializer")
public EventSerializer outboxEventSerializer(
        @Autowired(required = false)
        @Qualifier("outboxObjectMapper") ObjectMapper qualified,
        ObjectProvider<ObjectMapper> primary) {
    ObjectMapper mapper =
        (qualified != null) ? qualified
        : primary.getIfAvailable(JacksonObjectMapperFactory::defaults);
    return new JacksonEventSerializer(mapper);
}
```

Users override in three ways, in increasing specificity:
- Customize Boot's primary `ObjectMapper` (affects every Jackson user in
  the application).
- Register a qualified `@Bean("outboxObjectMapper") ObjectMapper` —
  isolated to the event-outboxer.
- Register a fully custom `@Bean("outboxEventSerializer") EventSerializer`
  — bypasses Jackson entirely.

### DB schema

The column `payload JSONB NOT NULL` (not `BYTEA`). Readable in psql, GIN
indexing over payload fields is possible for debugging.

There is no `payload_format` column — not needed with a single serializer.

### General notes

- `payload_class VARCHAR(512)` is kept — the FQN of the Java class for
  strict deserialization.
- `Handler.payloadType()` returns `Class<T>` — Jackson's `readValue` uses
  it.

## Rationale

### Why Jackson

- De facto standard in the Java ecosystem (Spring Boot, REST APIs).
- Good evolution story:
  `@JsonIgnoreProperties(ignoreUnknown=true)`, default values for new
  fields, `@JsonAlias` — all standard practices.
- Readable in the DB (JSONB).
- Works with records and Lombok classes out of the box.

### Why NOT Kryo

Kryo 5.x broke backward compatibility with 4.x (reference resolution, class
registration). Events in the DB are long-lived (can sit for hours while
being retried or scheduled). A Kryo upgrade renders events unreadable.
Unacceptable for outbox use cases.

### Why NOT Java Native

Gadget chains are the main RCE vector in the JVM ecosystem. Storing
attacker-controlled bytes and deserializing them is a known vulnerability
pattern. Even if the payload is "trusted" today, a SQL injection or
database compromise would grant RCE to workers. Jackson with explicit type
info is significantly safer.

### Why NOT Protobuf/Avro in MVP

Requires schema-first design: `.proto`/`.avsc` files + generated classes.
Adds build complexity and cognitive overhead for the base case. If users
already have a protobuf infrastructure, we can add an
`event-outboxer-serializer-protobuf` module post-MVP.

### Why NOT Smile/CBOR/Fury

Binary formats. They lose DB readability (important for admin debugging).
The size savings are ~30% but outbox payloads are tiny — absolute savings
are small. We can add these post-MVP on request.

## Consequences

### For users

- The payload is any Jackson-serializable class (record, POJO, with
  Jackson annotations).
- Evolution through `ignoreUnknown` + defaults.
- Custom `ObjectMapper` through Spring `@Bean`.

### For maintainers

- `EventSerializer` SPI is minimal — if a second format appears, the SPI
  will expand (possibly a breaking change in MVP).
- The `payload` column is `JSONB` in the PG adapter.
- InMemory storage keeps the payload as a `String` (the output of
  `serialize()`).

### Positive consequences

- Simplicity.
- DB readability.
- Familiar evolution model.
- Minimum dependencies (`jackson-databind` + datatype modules).

### Negative consequences

- If a binary format is needed later, there will be a breaking change in
  the SPI.
- Slightly larger payload size vs binary alternatives.
- No schema registry — evolution discipline is up to the DTO author.

## Post-MVP paths

If the need arises:
1. Extend the `EventSerializer` SPI (e.g. add `byte[] serialize()`).
2. Add a `payload_format VARCHAR(64)` column to the schema.
3. Introduce `EventSerializerRegistry` and per-type routing.
4. Add modules: `-serializer-jackson-smile`, `-serializer-protobuf`,
   `-serializer-fury`.
5. `SerializerWithFallbackDeserializers` for format migration.

## Amendment (2026-07-26): shipped defaults reconciled with this ADR

The first implementation of `JacksonObjectMapperFactory.defaults()`
shipped the OPPOSITE of the mapper prescribed above: it enabled
`FAIL_ON_UNKNOWN_PROPERTIES` (and `FAIL_ON_NULL_FOR_PRIMITIVES`),
omitting `ADJUST_DATES_TO_CONTEXT_TIME_ZONE`. Combined with the
dispatcher insta-disabling events on deserialization failure, a rolling
deploy that added a DTO field permanently disabled every event of that
type claimed by an outdated replica.

The code is now reconciled with this ADR:

- `FAIL_ON_UNKNOWN_PROPERTIES` disabled — unknown fields are ignored,
  as the "good evolution story" rationale above always intended.
- `ADJUST_DATES_TO_CONTEXT_TIME_ZONE` disabled, per the snippet above.
- `FAIL_ON_NULL_FOR_PRIMITIVES` left at its default (disabled). An
  earlier draft of this amendment considered enabling it as a
  data-corruption guard, but for record DTOs (the norm per ADR-0003)
  Jackson routes an *absent* primitive component through the same null
  path as an explicit null — enabling the feature breaks the
  add-a-primitive-field evolution case this mapper must survive.
- Deserialization failures now route through the FailureHandler chain
  (retry with backoff, DISABLED only after the attempt budget) instead
  of finalizing to DISABLED on first failure — see ADR-0007 amendment.

Applications wanting strict deserialization opt in with their own
mapper (`@Bean("outboxObjectMapper")` in the starter).

## Amendment (2026-08-02): post-MVP paths implemented by ADR-0025

[ADR-0025](0025-binary-capable-serializer-spi-and-payload-format.md)
implements post-MVP paths 1–3 and 5 of this ADR while the library is
still pre-1.0 (the cheapest moment for the breaking SPI/schema change
this ADR predicted):

- The "Minimal SPI port" section is superseded: `EventSerializer` now
  has `format()` and works over the two-lane `SerializedPayload`
  (text/bytes) instead of `String`.
- The "DB schema" section is superseded: `payload` is nullable JSONB
  next to `payload_binary BYTEA` (CHECK: exactly one set) and a
  `payload_format VARCHAR(64) NOT NULL` column records the writing
  serializer, enabling registry-routed deserialization and format
  migration.
- The `payload_class` note is corrected: the FQCN was never read back
  for "strict deserialization" — the deserialization target is always
  `EventHandler.payloadType()` (ADR-0003); the column is publish-time
  diagnostics.

Everything else stands: **Jackson remains the only shipped
implementation** (path 4 — actual binary modules — still awaits real
demand), the ObjectMapper resolution order, the
`outboxEventSerializer` override contract, and the whole "Why
Jackson / why not X" rationale.

## Related decisions

- [ADR-0003](0003-explicit-dto-payload.md) — payload is an explicit DTO.
- [ADR-0012](0012-extract-lock-key-on-handler.md) — `lockKey` is derived
  from the deserialized payload (Jackson's cheap deserialization makes
  this acceptable).
- [ADR-0007](0007-failure-handler-chain-of-responsibility.md) —
  deserialization failures are handled by the failure chain.
