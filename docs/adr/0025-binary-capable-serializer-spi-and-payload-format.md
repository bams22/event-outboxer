# ADR-0025: Binary-capable serializer SPI and per-event payload format

## Status

Accepted

## Date

2026-08-02

## Context

ADR-0011 shipped the MVP serialization seam deliberately minimal: a
`String`-based `EventSerializer` SPI, a single Jackson implementation,
`payload JSONB NOT NULL` in PostgreSQL and no format metadata. Its own
"Post-MVP paths" section named the extensions that would be needed for a
binary format (Protobuf, Smile, Fury): a byte-capable SPI, a
`payload_format` column, an `EventSerializerRegistry`.

Revisiting the question pre-1.0 changed the cost calculus:

- The SPI signature and the DB schema are the two places where a later
  change is a **breaking** change for users (SPI: semver major; schema:
  a migration story for every installed base). Today, with `v0.1.0` /
  `v0.2.0` tags and japicmp still report-only, both are cheap to change.
- A binary serializer physically cannot be implemented against the old
  seam: `String serialize(...)` corrupts non-UTF-8 bytes (or forces a
  +33% Base64 detour), and PostgreSQL rejects non-JSON in a `JSONB`
  column.
- One user concern turned out to be already solved and only badly
  documented: renaming/moving a payload DTO does **not** break stored
  events. `payload_class` is write-only diagnostics; deserialization is
  always driven by `EventHandler.payloadType()` and the stable
  `event_type` key (ADR-0003). This ADR keeps that property and fixes
  the documentation and Javadoc that claimed otherwise.

Decided with the maintainer: prepare the architecture now, ship **no**
binary serializer module yet (Jackson remains the only implementation,
per ADR-0011's YAGNI stance); prove the seam with a binary test
serializer in the shared contract tests.

## Decision

### 1. SPI v2: format id + two-lane payload value type

```java
public interface EventSerializer {
    String format();                                    // e.g. "jackson-json"
    SerializedPayload serialize(Object payload);
    <T> T deserialize(SerializedPayload payload, Class<T> type);
}
```

`SerializedPayload` (in `io.github.bams22.outboxer.domain`, because the
domain records carry it) is a record with exactly one of two lanes set:

- `text` — textual formats (JSON). Storage adapters may persist it in a
  structured column (`JSONB`); round-trips are semantic, not
  byte-identical (JSONB canonicalizes whitespace and key order).
- `bytes` — binary formats. Persisted verbatim (`BYTEA`); round-trips
  are byte-exact.

`format()` is a stable, lowercase-kebab, ≤64-char id persisted with
every event. It must never be renamed once events written with it may
exist.

### 2. One write serializer, registry-routed reads

`EventSerializerRegistry` (in `-spi`) maps format id → serializer. The
publisher serializes every new event with the single configured **write
serializer** and stamps its `format()`; the dispatcher selects the
deserializer by the **stored** `payloadFormat` of the claimed event, not
by the write serializer. This makes format migrations and rolling
deploys safe: in-flight events keep routing to the serializer that
wrote them while new events already use the new format.

An unknown stored format raises `UnknownPayloadFormatException`
(**OUTBOX-203**, a `HandleException`) inside the dispatcher's existing
deserialization try-block, so it routes through the FailureHandler
chain like OUTBOX-202 (ADR-0007 amendment): retry with backoff — an
updated replica may pick the event up — and `DISABLED` only after the
attempt budget. Never an insta-DISABLE.

Plain-Java wiring: `OutboxEngineBuilder.eventSerializer(...)` keeps its
signature (= write serializer, auto-registered for reads); new
`additionalSerializers(...)` registers read-only formats.

### 3. Schema: dual payload columns + payload_format

Additive migrations V006 (events) and V007 (archive) — V001/V002 stay
untouched (tagged releases pin their Flyway checksums; V003–V005 set
the additive convention, V005 belongs to the lock module and Flyway
versions are global across locations):

```sql
ALTER TABLE ... ALTER COLUMN payload DROP NOT NULL;
ALTER TABLE ... ADD COLUMN payload_binary BYTEA;
ALTER TABLE ... ADD COLUMN payload_format VARCHAR(64);
UPDATE ... SET payload_format = 'jackson-json' WHERE payload_format IS NULL;
ALTER TABLE ... ALTER COLUMN payload_format SET NOT NULL;
ALTER TABLE ... ADD CONSTRAINT events_payload_exactly_one
    CHECK ((payload IS NULL) <> (payload_binary IS NULL));
```

JSON payloads keep the ADR-0011 benefits (psql readability, GIN
indexing); binary payloads are stored natively without Base64 bloat.
Liquibase changelogs gain changeSets delegating to the same SQL files.

### 4. Starter resolution of the write serializer

All `EventSerializer` beans are collected into the registry. The write
serializer resolves as:

1. `event-outboxer.serializer.write-format` property, if set (fail-fast
   when it matches no registered format);
2. the only bean, when exactly one is registered (zero-config default);
3. the bean named `outboxEventSerializer` — the documented override
   keeps winning, and adding a read-only serializer for migration never
   silently changes the writer;
4. otherwise startup fails listing the registered formats.

The `outboxEventSerializer` bean name and the ObjectMapper resolution
order of ADR-0011 are unchanged.

### 5. Domain and observability

- `PendingEvent` / `ClaimedEvent` / `Event` / `ArchivedEvent` carry
  `SerializedPayload payload` + `String payloadFormat`. `payloadClass`
  stays, re-documented as publish-time diagnostics only.
- `SerializationErrorInfo` now carries `payloadFormat`,
  `storedPayloadClass` (publish-time FQCN) and `targetType`
  (`handler.payloadType()`), fixing a bug where the "class the engine
  tried to deserialize into" was actually the publish-time class — the
  two diverge exactly when a DTO was renamed between publish and
  handle.
- Admin surfaces expose `payloadFormat`, and the payload as exactly one
  of `payload` (text, verbatim) or `payloadBase64` (binary).

### 6. Validation without a binary module

`BinaryTestEventSerializer` in the `-spi` test-jar encodes a fixed DTO
behind an invalid-UTF-8 magic prefix (`0x00 0xFF`). Contract tests
round-trip binary payloads byte-exact through the in-memory and
PostgreSQL stores; a core E2E test drives publish → claim → dispatch
with it; dispatcher tests cover stored-format routing and the
OUTBOX-203 path; PG ITs assert the lane exclusivity and the CHECK
constraint.

## Alternatives considered

- **Pure `byte[]` SPI + `isTextual()` flag**: rejected — the PG adapter
  would UTF-8-decode every textual write to bind JSONB (cost + charset
  ambiguity), admin surfaces lose the first-class text lane, and the
  "exactly one column" DB invariant would not be expressible in the
  type system the way `SerializedPayload`'s compact constructor does.
- **Single `BYTEA` column for everything**: rejected — loses psql
  readability and GIN indexing for JSON, which STORAGE.md documents as
  deliberate operational value.
- **Base64 inside the existing `JSONB` column**: rejected — +33% size,
  no native binary semantics, still no format metadata.
- **Per-event-type write serializer configuration**: rejected for now —
  the registry covers the read side; a per-type write override adds
  configuration surface without a demonstrated need. Can be layered on
  later without schema or SPI changes.
- **Do nothing until a binary format is demanded** (ADR-0011 status
  quo): rejected — post-1.0 the same change costs a semver major plus a
  user-facing migration story; pre-1.0 it costs neither.

## Consequences

### Positive

- A binary serializer module (`-serializer-protobuf`, `-serializer-fury`,
  …) is now purely additive: implement `EventSerializer`, register the
  bean, set `write-format`. Realized for Protobuf by
  [ADR-0026](0026-protobuf-serializer-module.md).
- Format migrations are safe by construction (stored-format routing) and
  need no per-event rewrite.
- The refactoring-safety property (DTO renames don't break stored
  events) is now documented and guarded by the corrected
  `SerializationErrorInfo`.

### Negative

- Breaking change pre-1.0: `EventSerializer` implementors and every
  constructor of the four domain records must adapt (CHANGELOG notes
  the migration).
- Two nullable payload columns instead of one NOT NULL column — the
  CHECK constraint carries the invariant.
- The registry adds one concept to the SPI surface.

## Related decisions

- [ADR-0011](0011-jackson-json-only-in-mvp.md) — superseded in its SPI
  shape and "no payload_format column" clauses; Jackson-only shipping
  and the ObjectMapper resolution order stand. Amended.
- [ADR-0003](0003-explicit-dto-payload.md) — the single-entry allow-list
  (`handler.payloadType()`) remains the deserialization target.
- [ADR-0007](0007-failure-handler-chain-of-responsibility.md) —
  OUTBOX-203 joins OUTBOX-202 in the failure chain.
- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — the port list
  gains `format()` and `EventSerializerRegistry`.
- [ADR-0026](0026-protobuf-serializer-module.md) — the first shipped
  binary serializer module built on this seam.
