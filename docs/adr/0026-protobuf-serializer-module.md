# ADR-0026: Protobuf serializer module

## Status

Accepted

## Date

2026-08-02

## Context

ADR-0011 deferred binary formats out of the MVP and named
`event-outboxer-serializer-protobuf` as the module that would carry
Protobuf if demand appeared. ADR-0025 then rebuilt the serialization
seam so that a binary module is "purely additive: implement
`EventSerializer`, register the bean, set `write-format`" — proving the
byte lane end-to-end with a test-only binary serializer but shipping no
real one.

This ADR ships the first real binary serializer. The decision points
were (a) how payload DTOs map to Protobuf, and (b) how the build obtains
generated message classes for the module's own tests.

## Decision

### 1. Format id: `protobuf`

`ProtobufEventSerializer.FORMAT = "protobuf"` — lowercase kebab, ≤64
chars, stamped into `payload_format` on every event this serializer
writes. Per ADR-0025 it is permanent: never renamed once events written
with it may exist.

### 2. Schema-first: payloads are generated `Message` classes only

The serializer accepts exactly the protoc-generated
`com.google.protobuf.Message` subclasses:

- `serialize(payload)` — `payload instanceof Message` →
  `SerializedPayload.ofBytes(message.toByteArray())`; anything else
  raises `PublishSerializationException` (OUTBOX-102) at publish time.
- There is no runtime schema generation for arbitrary POJOs/records.
  A generated message IS the explicit DTO that ADR-0003 requires; the
  `.proto` file is its schema.

### 3. Deserialization via cached static `parser()` lookup

`deserialize(payload, type)` resolves the generated static
`type.parser()` accessor once via reflection and caches the resulting
`Parser` in a `ConcurrentHashMap<Class<?>, Parser<?>>`. Parsers are
stateless and thread-safe, so one serializer instance serves all event
types concurrently (the SPI thread-safety contract). A non-`Message`
target type or a parse failure raises `PayloadDeserializationException`
(OUTBOX-202) through the FailureHandler chain; text-lane input
surfaces as `IllegalStateException` (`requireBytes()`), mirroring the
Jackson adapter's behavior for byte-lane input.

### 4. `ExtensionRegistryLite` as the constructor collaborator

`ProtobufEventSerializer()` uses the empty registry (sufficient for
proto3, which has no extensions);
`ProtobufEventSerializer(ExtensionRegistryLite)` lets proto2 extension
users have their extensions parsed instead of landing in the
unknown-field set. This mirrors how the Jackson adapter takes a
caller-supplied `ObjectMapper`.

### 5. Additive starter autoconfiguration

`ProtobufSerializerAutoConfiguration` registers bean
`outboxProtobufEventSerializer` — deliberately NOT
`outboxEventSerializer` — guarded by `@ConditionalOnClass` on the
adapter and `com.google.protobuf.Message`, backing off on a
user-defined `ProtobufEventSerializer` bean **by type** (two beans
would collide on the `protobuf` format id in the registry). An optional
`ExtensionRegistryLite` bean is injected when present.

Consequences under the ADR-0025 resolution rules:

- Jackson + protobuf, no config → Jackson's
  `outboxEventSerializer`-named bean keeps writing (rule 3); protobuf
  registers read-only. Adding the module never silently changes what
  writes.
- `event-outboxer.serializer.write-format=protobuf` switches the
  writer (rule 1).
- Protobuf-only setups (no Jackson module) write protobuf with zero
  config (rule 2).

### 6. protoc runs at build time for tests only

`io.github.ascopes:protobuf-maven-plugin` (goal `generate-test`)
compiles test-only `.proto` fixtures from `src/test/protobuf` into
`target/generated-test-sources`. The published jar contains **no**
generated code and no `.proto` files; consumers bring their own
generated classes and never need protoc through this library.

### 7. Version pinning in the parent

`protobuf-java` is not managed by `spring-boot-dependencies`; the
parent pom pins `protobuf.version` (dependencyManagement) and points
the plugin's protoc at the same version, because protobuf 4.x requires
generated code to be no newer than the runtime.

### 8. Legacy test literals renamed

Tests that used `"protobuf"` as a deliberately-unknown format id
(registry, dispatcher routing, starter resolution) now use
`"no-such-format"` — `protobuf` is a real registered format.

## Alternatives considered

- **Runtime schemas for arbitrary POJOs** (protostuff,
  `jackson-dataformat-protobuf`): rejected — reintroduces
  reflection-derived implicit schemas against the spirit of ADR-0003,
  adds a less canonical dependency, and evolution guarantees would
  depend on field ordering heuristics instead of explicit field
  numbers.
- **`getDefaultInstance().getParserForType()` instead of `parser()`**:
  equivalent at runtime; `parser()` is the documented public accessor
  and needs one reflective call instead of two.
- **Checking generated test classes into the repo**: rejected —
  generated code drift; the build plugin keeps fixtures honest against
  the pinned protoc.
- **Well-known types as test fixtures (no protoc)**: rejected — cannot
  express the v1/v2 same-message evolution cases that prove
  unknown-field tolerance and retention.

## Consequences

### Positive

- ADR-0025's format-migration recipe is now executable end-to-end with
  a shipped second format; the byte lane (`payload_binary BYTEA`,
  byte-exact round-trips) gets a real production user.
- Protobuf-only applications get a zero-config writer.
- Schema evolution moves to Protobuf's explicit field-number
  discipline; unknown fields are tolerated and retained across
  mixed-version rolling deploys (proto3 unknown-field retention).

### Negative

- Payloads are not psql-readable (admin surfaces expose
  `payloadBase64`), as ADR-0011 predicted for binary formats.
- The library pins a `protobuf-java` version consumers must tolerate
  (overridable via their own dependencyManagement).
- Schema-first workflow: users must maintain `.proto` files and a
  protoc step in their own build.

## Related decisions

- [ADR-0003](0003-explicit-dto-payload.md) — a generated message is the
  explicit DTO; the deserialization target stays
  `EventHandler.payloadType()`.
- [ADR-0011](0011-jackson-json-only-in-mvp.md) — amended: post-MVP
  path 4 (a real binary module) is now realized; Jackson remains the
  default writer.
- [ADR-0016](0016-maven-module-structure.md) — amended: 19th module,
  package `io.github.bams22.outboxer.serializer.protobuf.*`.
- [ADR-0025](0025-binary-capable-serializer-spi-and-payload-format.md)
  — the seam this module plugs into; its resolution rules are
  unchanged.
