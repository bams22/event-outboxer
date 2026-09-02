# ADR-0032: Spring Cloud Stream relay module

## Status

Accepted

## Date

2026-09-02

## Context

Every service that uses the outbox to publish to a message broker ends
up writing the same two classes: a DTO that carries (destination, key,
headers, payload) and an `EventHandler` that forwards it through the
broker client. ADR-0001 deliberately keeps cross-service delivery out
of the engine — "the user's `EventHandler` publishes to the broker" —
but nothing stops the library from *shipping* that handler once,
well-tested, instead of letting every project re-implement it.

Spring Cloud Stream is the natural integration point: `StreamBridge`
abstracts the binder (Kafka, RabbitMQ, ...), so one relay module covers
every broker Spring Cloud Stream supports. The engine already provides
everything the relay needs for free: transactional publish (ADR-0002),
retries through the FailureHandler chain (ADR-0007), per-type tuning,
coalescing (ADR-0021), scheduled delivery, trace-context capture and
restoration (ADR-0023), metrics and admin surfaces.

The decision points were: (a) where a Spring Cloud Stream dependency
may live given invariant 9 ("Spring only in the starter and admin
modules"), (b) one worker per binding vs one shared relay event type
(ADR-0004 tension), (c) how the user payload is stored — it must remain
an explicit DTO story (ADR-0003) without polymorphic deserialization,
and (d) how the broker message key is conveyed without hard-coding a
binder.

## Decision

### 1. A fourth Spring surface: `event-outboxer-relay-spring-cloud-stream`

New module, package `io.github.bams22.outboxer.relay.stream`,
properties under `event-outboxer.relay.stream.*`. Like the admin
modules (ADR-0019) it is a Spring surface by nature and self-wires
through its own `AutoConfiguration.imports`; the starter knows nothing
about it. It depends on `-api` only (no `-spi` — it uses no ports), and
the `ban-core-in-adapter` enforcer rule applies — scope-qualified,
because the module's end-to-end test deliberately runs the full engine
via the starter at test scope. Invariant 9 now reads: Spring appears in
the starter, the two admin surface modules, and this relay module.

The parent pom imports `spring-cloud-dependencies` (release train
2025.0.x, the line compatible with Boot 3.5.x) after
`spring-boot-dependencies`, so Boot wins on overlaps. The consumer BOM
does not re-export the train — applications bring their own.

### 2. One event type: `outboxer-stream-relay`, binding inside the envelope

All bindings share a single relay event type; the binding name is a
field of the envelope, not part of the type. `outboxer-stream-relay` is
a library-reserved persisted natural key, deliberately decoupled from
the module name and never to change. The trade-off against ADR-0004 is
explicit: all relayed messages share one worker pool and one poller, so
a slow binding can delay others. Accepted for now — retry tuning is
still available through the regular per-type configuration
(`event-outboxer.event-types.overrides.outboxer-stream-relay`), and
per-binding event types remain an open follow-up if isolation is ever
needed.

The name is kebab-case with **no dot**, and that is a deliberate
constraint rather than a style choice. `overrides` binds to a
`Map<String, OutboxProperties.EventType>` whose values are structured,
and Spring Boot's map binder splits such keys on `.`; a name like
`outboxer.stream-relay` would only bind when written in brackets
(`"[outboxer.stream-relay]"`), because YAML quoting is stripped by the
parser before the binder sees the key. Since override keys are not
validated against the registered handlers, the un-bracketed form would
fail silently — the operator sees their retry tuning in `application.yml`
and the relay quietly keeps running on `defaults`. A hyphenated name
binds identically in every notation (bare, quoted, bracketed, flat
`.properties`), so the whole failure mode disappears. The rule applies
to any event type an application names, not just this one; see
CONFIGURATION.md § "Event-type names containing a dot".

The module reserves message code `OUTBOX-105`
(`StreamEncodingException`).

### 3. The envelope is a persisted schema

`StreamEnvelope(binding, key, headers, contentType, textPayload,
binaryPayload)` is stored as a regular event payload through the
engine's `jackson-json` write format. It is treated like a database
migration: component names are the JSON field names of stored rows and
are never renamed; components added after this release must be
`@Nullable` (or defaulted) so older rows keep deserializing; the
compact constructor never starts requiring a new field. A round-trip
test pins the field names and both lanes.

Because the envelope is a POJO record, a relay deployment requires the
`jackson-json` write format for this type — the module therefore
depends on `event-outboxer-serializer-jackson` outright (which the
starter already ships non-optionally, ADR-0016 amendment 2026-08-29).
Deployments with `write-format=protobuf` must add
`event-outboxer.serializer.write-format-per-type.outboxer-stream-relay:
jackson-json`.

### 4. Payload is encoded at publish time, delivered verbatim

`StreamOutboxPublisher.publish(...)` encodes the user payload to its
wire form inside the caller's transaction and stores it in the
envelope; the handler ships the stored form untouched. Consequences:
encoding failures surface synchronously as `StreamEncodingException`
(nothing persisted), what was captured in the transaction is exactly
what reaches the broker, and the handler needs no knowledge of user
classes — no polymorphic deserialization (ADR-0003 stays intact).

The wire form travels in two lanes mirroring `SerializedPayload`:
`textPayload` for textual formats, `binaryPayload` for binary ones
(base64 inside the JSONB column — documented overhead). Encoding is an
SPI: `StreamPayloadEncoder`, default `JacksonStreamPayloadEncoder`
(JSON, `ObjectMapper` resolved through the same chain as the Jackson
event serializer: `outboxObjectMapper` bean → primary mapper → library
defaults). Pre-encoded payloads pass through without the encoder: a
`String`, `byte[]` or `SerializedPayload` payload IS the wire form —
a `String` is not JSON-quoted, and pre-encoded payloads should carry an
explicit `contentType` (fallback:
`event-outboxer.relay.stream.default-content-type`).

### 5. Configurable message-key header, Kafka default

The handler writes the envelope key into one configurable header,
`event-outboxer.relay.stream.message-key-header`, default
`kafka_messageKey` (the Kafka binder's record-key header — kept as a
string constant, no spring-kafka dependency), encoded as UTF-8 bytes to
match the binder's default `ByteArraySerializer`. An empty value
disables the header entirely for setups that rely on the binding's
`partitionKeyExpression`. Envelope headers are copied first; the
explicit `contentType` and key headers win on collision.

### 6. Opt-in per-key ordering through ADR-0012 lock keys

`event-outboxer.relay.stream.per-key-ordering=true` makes the handler's
`extractLockKey` return `outboxer-stream-relay:<binding>:<key>` (namespaced —
lock keys are global across event types), serializing handling of
events that share a (binding, key) pair. Requires a real `EntityLocker`
(`event-outboxer.lock.type`); default off. This orders the relay's
sends, which is stronger than batch-local grouping but still subject to
at-least-once redelivery (ADR-0015) — consumers keep deduplicating.

### 7. Active by default, kill switch provided

Presence of the module plus `StreamBridge` on the classpath activates
the relay (`matchIfMissing=true`), following the admin-actuator
posture: the jar has exactly one purpose and opens no HTTP surface, so
a second switch would be ceremony.
`event-outboxer.relay.stream.enabled=false` disables it. The facade
bean backs off silently when no `OutboxEventPublisher` bean exists
(engine disabled); user-defined encoder/handler/facade beans win via
`@ConditionalOnMissingBean`. Registering an *independent*
`EventHandler<StreamEnvelope>` bean instead of overriding the handler
bean fails startup with a duplicate-handler error.

### 8. Durability stops at `StreamBridge.send`: the acknowledged send is the application's responsibility

The handler treats `send(...) == true` as delivery and returns
`EventOutcome.success()`; the engine then finalizes the event and the
outbox row is gone. Whether that boolean means "the broker has the
message" depends entirely on the binder, and the common defaults say
no: the Kafka binder's producer property `sync` defaults to `false`,
so `send` returns once the record sits in the producer's buffer, ahead
of the broker acknowledgement. A crash or an async producer error in
that window loses the message with no outbox row left to retry from —
the relay is at-most-once on that hop, which is precisely the gap the
outbox is meant to close.

We do not enforce this from code. Forcing a synchronous send would
mean either reaching into binder-specific producer properties (the
binder-neutrality this ADR is built on, see §5) or blocking on a
future the `StreamOperations` contract does not expose. Instead the
requirement is documented as a first-class production prerequisite in
the module doc and CONFIGURATION.md: set
`spring.cloud.stream.kafka.bindings.<binding>.producer.sync: true`
(or the binder's equivalent — RabbitMQ publisher confirms, etc.).
Revisit if a binder-neutral "await ack" hook appears upstream.

## Alternatives considered

- **Per-binding event types** (`outboxer-stream-relay:<binding>`), one
  worker pool per binding per ADR-0004: rejected for now — handlers are
  registered at startup while bindings are data, the configuration
  surface grows considerably, and the single-type design can be
  extended later without breaking stored events.
- **Storing the raw payload object and serializing at handle time**:
  rejected — requires the payload class on the consumer path and
  polymorphic deserialization (security and ADR-0003 violations), and
  delivery could fail on schema drift after the transaction committed.
- **A module-owned scheduler/port pair** (publish + poll + mark
  published as one interface, a scheduled task pushing to the broker):
  rejected — duplicates the engine (claiming, retries, heartbeat,
  observability) with weaker guarantees; the whole point of the module
  is that the engine already does this.
- **Optional (`provided`) Spring Cloud Stream dependency inside the
  starter**: rejected — the starter would grow binder knowledge and a
  conditional surface for something only some applications use;
  a self-wiring module keeps the starter honest (ADR-0019 precedent).
- **`KafkaHeaders.KEY` constant via spring-kafka**: rejected — a
  compile dependency on one binder to name one header; the string plus
  a property is binder-neutral.

## Consequences

### Positive

- One dependency replaces a per-project DTO + handler + wiring; the
  facade is a two-line call.
- The relay inherits the engine's machinery wholesale: transactional
  publish, FailureHandler retries, coalescing via `PublishOptions`,
  `runAt`, tracing (ADR-0023 — the send runs inside the restored trace
  context), metrics and admin visibility under one event type.
- Binder-agnostic: anything Spring Cloud Stream binds, the relay
  reaches.
- Relayed payload stays readable in `psql` for the common JSON case
  (text lane inside JSONB).

### Negative

- One worker pool for all bindings (see §2) — a slow binding delays the
  rest until per-binding isolation is built.
- A new release-train pin (`spring-cloud.version`) to keep aligned with
  the Boot baseline.
- The envelope schema is a forever-contract; evolving it takes the same
  discipline as a DB migration.
- Binary payloads pay the base64 tax inside JSONB.
- At-least-once delivery to the broker: duplicates are possible and
  consumer-side deduplication remains required (ADR-0015) — the module
  does not change semantics, only packaging.
- That at-least-once guarantee holds only with an acknowledged
  producer send (§8). On binder defaults — Kafka's `sync: false` — the
  outbox-to-broker hop is at-most-once, and the requirement lives in
  documentation rather than in code.
- The reserved type name contains a dot, so its per-type configuration
  needs Boot's bracket key form (§2); the dotted form is a silent
  no-op.

## Related decisions

- [ADR-0001](0001-local-embedded-outbox-scope.md) — amended: scope
  unchanged; the relay is a packaged implementation of the documented
  "user handler publishes to the broker" pattern, not a cross-service
  bridge.
- [ADR-0002](0002-participate-in-client-transaction.md) — the facade
  delegates to `OutboxEventPublisher`, inheriting transaction
  participation verbatim.
- [ADR-0003](0003-explicit-dto-payload.md) — the envelope is the
  explicit DTO; publish-time encoding avoids polymorphic
  deserialization.
- [ADR-0004](0004-per-event-type-worker-isolation.md) — consciously
  traded away for relay events (single shared type, §2).
- [ADR-0012](0012-extract-lock-key-on-handler.md) — per-key ordering
  rides on handler lock keys.
- [ADR-0015](0015-at-least-once-semantics.md) — delivery to the broker
  is at-least-once; consumers deduplicate.
- [ADR-0016](0016-maven-module-structure.md) — amended: 20th module,
  package `io.github.bams22.outboxer.relay.stream`.
- [ADR-0019](0019-admin-and-retention-surface.md) — the self-wiring
  Spring-surface precedent this module follows.
- [ADR-0023](0023-tracing-spi-port-and-adapters.md) — why the relay
  needs no tracing code of its own.
- [ADR-0025](0025-binary-capable-serializer-spi-and-payload-format.md)
  — `SerializedPayload` supplies the dual-lane pattern the envelope
  mirrors; the per-type write-format override enables protobuf-writer
  deployments (§3).
