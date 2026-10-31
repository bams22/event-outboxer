# event-outboxer-relay-spring-cloud-stream

A ready-made broker relay
([ADR-0032](../adr/0032-spring-cloud-stream-relay-module.md)): the
`StreamOutboxPublisher` facade stores a message (binding, key, headers,
payload) in the outbox inside your transaction, and a built-in
`EventHandler` delivers it to the broker through Spring Cloud Stream's
`StreamBridge` — no per-project event DTO or handler to write.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-relay-spring-cloud-stream` |
| Java package | `io.github.bams22.outboxer.relay.stream` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md), [`event-outboxer-serializer-jackson`](event-outboxer-serializer-jackson.md), `spring-cloud-stream` (you add the binder) |
| Enable with | module on the classpath + `StreamBridge` present — requires acknowledged delivery configuration; `event-outboxer.relay.stream.enabled=false` to opt out |

## Why it exists

The most common outbox handler by far is "publish this to the broker".
[ADR-0001](../adr/0001-local-embedded-outbox-scope.md) keeps
cross-service delivery out of the engine — the handler publishes to a
broker — but every project was writing that same handler and its DTO by
hand. This module ships them once: publish through the facade, and the
engine's whole machinery (transactional publish, FailureHandler
retries, coalescing, scheduled delivery, tracing, metrics, admin) works
for broker messages out of the box.

## What it does

**`StreamOutboxPublisher`** (facade, registered automatically):

```java
// the common case — binding, key, payload:
streamOutboxPublisher.publish("orders-out", order.id(), new OrderCreated(...));

// the full form — headers, contentType, PublishOptions passthrough:
streamOutboxPublisher.publish(StreamOutboxMessage.builder()
        .binding("orders-out")
        .key(order.id())
        .headers(Map.of("x-tenant", tenantId))
        .payload(new OrderCreated(...))
        .options(PublishOptions.builder().coalescingKey(order.id()).build())
        .build());
```

The payload is encoded to its wire form **at publish time** (JSON via
the resolved `ObjectMapper` by default; replace the
`StreamPayloadEncoder` bean for other formats) and stored in a
`StreamEnvelope` event under the reserved type `outboxer-stream-relay`.
A `String`, `byte[]` or `SerializedPayload` payload passes through as
the wire form itself — pair it with an explicit `contentType`.
Encoding failures throw `StreamEncodingException` (OUTBOX-105) inside
your transaction; nothing is persisted.

**`StreamRelayEventHandler`** (built-in, picked up by the engine like
any handler) delivers stored envelopes: copies the envelope headers,
sets `contentType`, writes the key into the configured key header
(default `kafka_messageKey`, UTF-8 bytes), and calls
`StreamBridge.send(binding, message)`. A `false` return or an exception
routes through the regular FailureHandler chain — delivery is
at-least-once ([ADR-0015](../adr/0015-at-least-once-semantics.md)),
consumers must deduplicate.

All bindings share the one relay event type (one worker pool, one
poller); retry policy and pool sizing are tuned via the regular
per-type mechanism:

```yaml
event-outboxer:
  event-types:
    overrides:
      outboxer-stream-relay:
        handler-pool-size: 8
        failure:
          max-attempts: 12
```

The reserved name is kebab-case with no dot on purpose, so it binds in
every notation — see [CONFIGURATION.md §Event-type names containing a
dot](../CONFIGURATION.md#event-type-names-containing-a-dot) for what a
dotted name would have cost.

## Delivery guarantees and migration

The handler requires an `AcknowledgedStreamSender`. Its `sendAndAwait` method must return
`true` only after broker acknowledgement, and return `false` or throw on timeout, rejection,
or an indeterminate result. Waiting must be bounded. The outbox retains/retries the event
until this contract is satisfied. A crash after acknowledgement and before finalization can
still redeliver it: consumers should deduplicate using the stable `outboxer_eventId` header,
which the relay writes from `EventContext.eventId()` on every attempt, overriding a caller header.

The built-in adapter supports the standard, nontransactional Kafka binder with explicit
synchronous output bindings. Startup rejects missing acknowledgements, and sends to unlisted
dynamic bindings fail instead of silently finalizing the event:

```yaml
event-outboxer:
  relay:
    stream:
      acknowledged-bindings: [orders-out]
      serialize-per-key: false
spring:
  cloud:
    stream:
      default-binder: kafka
      kafka:
        bindings:
          orders-out:
            producer:
              sync: true
              configuration:
                acks: all
```

For RabbitMQ, named binder environments, transactional Kafka, or a customized asynchronous
StreamBridge/channel pipeline, provide an `AcknowledgedStreamSender` bean that awaits the
actual broker confirms/transaction commit. Merely wrapping asynchronous `StreamBridge.send`
is insufficient. The built-in adapter assumes the standard synchronous StreamBridge pipeline;
do not enable `StreamBridge.setAsync(true)` or replace its channels with asynchronous ones.
See the [Spring Kafka binder producer contract](https://docs.spring.io/spring-cloud-stream/reference/kafka/kafka_overview.html).

**Migration:** replace `StreamRelayEventHandler.builder().streamOperations(...)` with
`.acknowledgedSender(...)`. Applications using auto-configuration must supply the configuration
above or a confirming sender bean. The legacy `per-key-ordering` property and
`perKeyOrdering(...)` builder method remain deprecated aliases for `serialize-per-key` and
`serializePerKey(...)`. These provide mutual exclusion, never FIFO order across retries.

`PublishOptions.coalescingKey(...)` is the preferred name for the deprecated `dedupKey(...)`.
Use it only for replaceable work whose handler reads current state; payloads of swept events
are discarded. It is not an idempotency key for publication. The relay scopes this key by
binding using a SHA-256 digest of a length-prefixed tuple; identical user keys on different
outputs cannot coalesce. During a rolling upgrade old unscoped keys and new scoped keys may
produce additional runs, which remain covered by the at-least-once contract.

## When to use it

Any Spring Boot service that publishes domain events to Kafka /
RabbitMQ / any Spring Cloud Stream binder and wants outbox guarantees
without writing its own relay. Not for handlers with business logic —
those stay regular `EventHandler`s. Plain-Java (non-Spring) engines can
still construct both classes via their builders, but the module is a
Spring surface by design.

## How to configure it

### With Spring Boot

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-relay-spring-cloud-stream</artifactId>
</dependency>
<!-- plus your binder, e.g. spring-cloud-stream-binder-kafka -->
```

```yaml
event-outboxer:
  relay:
    stream:
      enabled: true                        # default — presence of the module is the opt-in
      message-key-header: kafka_messageKey # empty = no key header (use partitionKeyExpression)
      serialize-per-key: false              # true = serialize per (binding, key); needs lock.type
      default-content-type: application/json
```

Overriding beans: define your own `StreamPayloadEncoder`,
`StreamRelayEventHandler` or `StreamOutboxPublisher` bean and the
auto-configured one backs off. Do NOT register an independent
`EventHandler<StreamEnvelope>` — two handlers for one event type fail
startup.

If the global write format is not `jackson-json` (protobuf writer),
add:

```yaml
event-outboxer:
  serializer:
    write-format-per-type:
      outboxer-stream-relay: jackson-json
```

### Without Spring

```java
StreamRelayEventHandler relayHandler = StreamRelayEventHandler.builder()
        .acknowledgedSender(confirmedSender)
        .build();

new OutboxEngineBuilder()
        .handler(relayHandler)
        // ... storage, serializer, locker as usual
        .build();

StreamOutboxPublisher relay = DefaultStreamOutboxPublisher.builder()
        .outboxEventPublisher(outboxEventPublisher)
        .encoder(new JacksonStreamPayloadEncoder(objectMapper))
        .build();
```

## Related

- [ADR-0032](../adr/0032-spring-cloud-stream-relay-module.md) — the full design: envelope schema contract, single event type, key header, ordering.
- [ADR-0001](../adr/0001-local-embedded-outbox-scope.md) — scope: this is a packaged handler, not a cross-service bridge.
- [CONFIGURATION.md §`event-outboxer.relay.stream.*`](../CONFIGURATION.md#event-outboxerrelaystream) — property reference.
