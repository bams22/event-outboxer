# event-outboxer-testkit

Deterministic, single-threaded testing of your handlers and outbox
flows: no Testcontainers, no `@SpringBootTest`, no `Thread.sleep`.
The same tools the library uses to test itself.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-testkit` (**test scope**) |
| Java package | `io.github.bams22.outboxer.testkit` |
| Brings transitively | JUnit 5, AssertJ, the [in-memory adapter](event-outboxer-storage-inmemory.md), the [Jackson serializer](event-outboxer-serializer-jackson.md), [core](event-outboxer-core.md) |
| Spring | none — plain Java |

## Why it exists

Testing an asynchronous engine against real infrastructure means
timing hacks and flaky sleeps. The testkit replaces the two sources of
nondeterminism — background threads and the wall clock — with explicit
`tick()` calls and a settable clock, so a retry-after-backoff scenario
is three lines instead of a polling loop.

## What it contains

| Class | Role |
|---|---|
| `OutboxTestContext` (+ builder) | one-stop fixture: in-memory store/registry/locker, Jackson serializer, `SettableClock`, publisher, `ManualEngine`, `RecordingOutboxListener` pre-attached |
| `ManualEngine` | synchronous driver: `tick()` claims + dispatches **on the calling thread**; `tick(type, batchSize)`, `tickHeartbeat()`, `tickOrphanRecovery()`, `tickWatchdog()` |
| `SettableClock` | thread-safe mutable `Clock`: `advance(Duration)`, `set(Instant)`; statics `atSystemNow()`, `atEpoch()` |
| `RecordingOutboxListener` | captures all 21 `OutboxListener` callbacks into lists: `processed()`, `retryScheduled()`, `disabled()`, `stuckReclaimed()`, … plus `clear()` |
| `OutboxExtension` | JUnit 5 extension injecting a fresh default `OutboxTestContext` per test |
| `EventAssertions` / `EventStoreAssert` | AssertJ entry point `assertThatStore(store)`: `hasEvent(id).withStatus(…).withAttempts(…)`, `hasNoEvent(id)`, `hasTotalPending(n)`, … |

Context defaults are test-tuned: `no-transaction-policy=IGNORE`,
`WorkerId("test-worker")`, small poll intervals, `handlerPoolSize 1`,
1-minute `handlerMaxRuntime`/`lockTtl`, 1-second maintenance cadences.
Every builder setter mirrors an `event-outboxer.*` property or an
`OutboxEngineBuilder` option — including `writeSerializerOverride` for
per-type serializer tests.

## When to use it

- **Handler unit tests** and outbox-flow tests (retry chains, dedup,
  watchdog, orphan recovery) — the sweet spot.
- **Not** for verifying `@Transactional publish()` atomicity or SQL
  behaviour — that needs the real adapter: `@SpringBootTest` +
  Testcontainers with
  [storage-postgres](event-outboxer-storage-postgres.md), or
  `@Import(OutboxInMemoryTestConfiguration.class)` for DB-less Spring
  tests (see [TESTING.md](../TESTING.md#interop-with-springboottest)).
- Adapter authors testing a *new backend* want the SPI contract tests
  instead ([event-outboxer-spi](event-outboxer-spi.md#the-contract-test-kit))
  — the testkit tests *your application*, the contract tests test
  *the adapter*.

## How to use it

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-testkit</artifactId>
    <scope>test</scope>
</dependency>
```

### Quick start — extension-injected context

```java
@ExtendWith(OutboxExtension.class)
class OrderHandlerTest {

  @Test
  void sendsConfirmationEmail(OutboxTestContext outbox) {
    outbox.publisher().publish("SEND_EMAIL", new EmailRequest("me@x.io"));

    int dispatched = outbox.manualEngine().tick();   // synchronous, this thread

    assertThat(dispatched).isEqualTo(1);
    assertThatStore(outbox.eventStore()).hasTotalPending(0);
    assertThat(outbox.recording().processed()).hasSize(1);
  }
}
```

The extension-provided context has **no handlers registered** — for
handler tests build the context yourself:

```java
OutboxTestContext ctx = OutboxTestContext.builder()
    .handler(new SendEmailHandler(mockMailer))
    .clock(SettableClock.atEpoch())
    .build();
```

### Retry-then-disable — time travel instead of sleeps

```java
OutboxTestContext ctx = OutboxTestContext.builder()
    .defaultFailureHandler(FailureHandlers.<Object>builder()
        .withMaxAttempts(3, MaxRetriesFailureHandler.ExhaustedAction.DISABLE)
        .withFixedDelay(Duration.ofSeconds(1)))
    .handler(simpleHandler("FLAKY", p -> { throw new RuntimeException("nope"); }))
    .clock(SettableClock.atEpoch())
    .build();

UUID id = ctx.publisher().publish("FLAKY", "payload");
for (int i = 0; i < 3; i++) {
  ctx.manualEngine().tick();                       // one attempt
  ctx.clock().advance(Duration.ofSeconds(2));      // make the retry eligible
}

assertThatStore(ctx.eventStore())
    .hasEvent(id).withStatus(EventStatus.DISABLED).withAttempts(3);
assertThat(ctx.recording().retryScheduled()).hasSize(2);
assertThat(ctx.recording().disabled()).hasSize(1);
```

Maintenance flows are ticked the same way: `tickWatchdog()` after
advancing past `handlerMaxRuntime` asserts stuck-handler reclaim;
`registerWorker()` / `deregisterWorker()` + `tickOrphanRecovery()`
cover the orphan path. More recipes in [TESTING.md](../TESTING.md).

## Related

- [TESTING.md](../TESTING.md) — the full guide with three recipe patterns.
- [event-outboxer-storage-inmemory](event-outboxer-storage-inmemory.md) — the fixture layer underneath.
- [OBSERVABILITY.md §OutboxListener callback catalogue](../OBSERVABILITY.md#outboxlistener-callback-catalogue) — what `recording()` captures.
