# event-outboxer-serializer-protobuf

Schema-first Protobuf payload serializer: format id **`protobuf`**,
bytes lane. Payloads must be protoc-generated
`com.google.protobuf.Message` classes
([ADR-0026](../adr/0026-protobuf-serializer-module.md)); they persist
byte-exact in the `payload_binary BYTEA` column.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-serializer-protobuf` |
| Java package | `io.github.bams22.outboxer.serializer.protobuf` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-spi`](event-outboxer-spi.md), `protobuf-java` (4.x, pinned by the parent — generated code must not be newer than the runtime) |
| Format id | `protobuf` (stamped into `payload_format`, never renamed) |

## Why it exists

For high-volume event types JSON pays a real tax in payload size and
parse cost, and some teams already own `.proto` schemas as their
contract source of truth. This module adds a compact, schema-first
lane without touching the engine: the binary-capable serializer SPI
([ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md))
routes each event to the serializer matching its stored
`payload_format`, so Protobuf events and
[Jackson](event-outboxer-serializer-jackson.md) events coexist in one
table.

## What it does

**`ProtobufEventSerializer`** — the module's only public class:

- `serialize` accepts **only** `com.google.protobuf.Message`
  instances (`message.toByteArray()` → `SerializedPayload.ofBytes`).
  Anything else throws `PublishSerializationException` — there is no
  runtime schema generation for POJOs/records; the generated message
  *is* the explicit DTO [ADR-0003](../adr/0003-explicit-dto-payload.md)
  requires, its `.proto` file is the schema.
- `deserialize` targets the handler's `payloadType()` (which must be a
  generated `Message` class); the static `parser()` is resolved once
  via reflection and cached per class.
- Two constructors: no-arg (empty `ExtensionRegistryLite` — fine for
  proto3) and `ProtobufEventSerializer(ExtensionRegistryLite)` for
  proto2 extensions.
- Evolution semantics follow protobuf: unknown fields from newer
  writers are tolerated *and preserved through re-serialization*;
  removed fields read as defaults.
- Trade-off: payloads are not psql-readable; the
  [admin surfaces](event-outboxer-admin-actuator.md) expose them as
  `payloadBase64`.

The published jar ships no generated code and no `.proto` files —
consumers compile their own messages; the library never requires
protoc.

## When to use it

- Hot or large event types where JSON overhead is measurable.
- Contract-first teams that already version `.proto` files.
- Not worth it for low-volume types — Jackson's readable payloads make
  operations (psql debugging, admin views) easier.

## How to configure it

### Next to Jackson (gradual adoption — recommended path)

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-serializer-protobuf</artifactId>
</dependency>
```

Adding the module never silently changes what writes: the
auto-configured bean is named `outboxProtobufEventSerializer`
(deliberately *not* `outboxEventSerializer`), so it registers
**read-only** and Jackson keeps writing. Then route per type or flip
the default:

```yaml
event-outboxer:
  serializer:
    # move one type at a time (ADR-0025 amendment):
    write-format-per-type:
      ORDER_CREATED: protobuf
    # ...or switch the global writer:
    # write-format: protobuf
```

Reads always route by each event's stored `payload_format`, so
in-flight Jackson events drain with Jackson while new events write
Protobuf — no data rewrite, safe during rolling deploys. The full
migration recipe is in
[CONFIGURATION.md §Migrating between payload formats](../CONFIGURATION.md#migrating-between-payload-formats).

### Protobuf-only setup

Omit the Jackson serializer module; the single registered serializer
writes with zero configuration.

### Customization

- proto2 extensions: provide an `ExtensionRegistryLite` bean — the
  auto-configuration injects it.
- Full control: declare your own `ProtobufEventSerializer` bean; the
  auto-configuration backs off by type (two beans would collide on the
  `protobuf` format id).

### Without Spring

```java
new OutboxEngineBuilder()
    .eventSerializer(new JacksonEventSerializer(mapper))            // default writer
    .writeSerializerOverride("ORDER_CREATED", new ProtobufEventSerializer())
    // override serializers are auto-registered for reads
    .build();
```

### Handler side

```java
public class OrderCreatedHandler implements EventHandler<OrderCreatedProto> {
    @Override public String eventType() { return "ORDER_CREATED"; }
    @Override public Class<OrderCreatedProto> payloadType() { return OrderCreatedProto.class; }
    // publisher.publish("ORDER_CREATED", OrderCreatedProto.newBuilder()...build());
}
```

## Related

- [event-outboxer-serializer-jackson](event-outboxer-serializer-jackson.md) — the default text-lane serializer.
- [CONFIGURATION.md §Serialization](../CONFIGURATION.md#serialization) — write-format resolution rules.
- ADRs: [0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md), [0026](../adr/0026-protobuf-serializer-module.md).
