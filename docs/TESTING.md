# Testing handlers with `event-outboxer-testkit`

`event-outboxer-testkit` is a test-scope library that provides
deterministic, single-threaded control over the outbox engine. It is
what the library uses to test itself; application authors get the
same tools for their own handler tests — no Testcontainers, no
`@SpringBootTest`, no timing hacks.

## Add the dependency

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-testkit</artifactId>
    <scope>test</scope>
</dependency>
```

The testkit brings JUnit 5, AssertJ, the in-memory storage adapter and
the Jackson serializer transitively — nothing else to wire.

## One-minute quick start

```java
@ExtendWith(OutboxExtension.class)
class OrderHandlerTest {

  static final EventType<EmailRequest> SEND_EMAIL = EventType.of("SEND_EMAIL", EmailRequest.class);

  @Test
  void sendsConfirmationEmail(OutboxTestContext outbox) {
    // publish (typed key — the same constant a handler would return from type())
    outbox.publisher().publish(SEND_EMAIL, new EmailRequest("me@x.io"));

    // process synchronously on the current thread
    int dispatched = outbox.manualEngine().tick();
    assertThat(dispatched).isEqualTo(1);

    // assert the state
    assertThatStore(outbox.eventStore()).hasTotalPending(0);
    assertThat(outbox.recording().processed()).hasSize(1);
  }
}
```

The extension injects a fresh `OutboxTestContext` per test. Default
wiring: in-memory store, in-memory worker registry, noop locker,
Jackson serializer, `SettableClock.atSystemNow()`, empty handler set,
`no-transaction-policy=IGNORE`, `RecordingOutboxListener` pre-attached.

## Components

### `OutboxTestContext` — builder + facade

The extension uses the default builder. If you need handlers or
custom configuration, build a context yourself and skip the extension
(or stop auto-wiring and customise via `@BeforeEach`):

```java
OutboxTestContext outbox = OutboxTestContext.builder()
    .handler(new SendEmailHandler(mockMailer))
    .defaultEventTypeConfig(
        EventTypeConfig.defaults().toBuilder()
            .handlerMaxRuntime(Duration.ofMillis(50))
            .build())
    .clock(SettableClock.atEpoch())
    .build();
```

Every collaborator the engine uses is exposed on the context so your
assertions can reach it: `eventStore()`, `workerRegistry()`,
`entityLocker()`, `clock()`, `publisher()`, `manualEngine()`,
`recording()` (the `RecordingOutboxListener`), `workerInfo()`.

### `SettableClock`

A thread-safe mutable `Clock`. Used by every collaborator in the
context so you can time-travel without `Thread.sleep`.

```java
outbox.clock().advance(Duration.ofSeconds(10));
outbox.clock().set(Instant.parse("2026-05-01T12:00:00Z"));
```

### `ManualEngine`

Synchronous step-through driver. Unlike `OutboxEngine`, nothing runs
on background threads — every `tick()` call claims + dispatches a
batch on the calling thread, deterministically.

| Method | What it does |
|---|---|
| `tick()` | Claim + dispatch for every registered type; returns total dispatched. |
| `tick(type, batchSize)` | Claim + dispatch only for one type. |
| `tickHeartbeat()` | One heartbeat write. |
| `tickOrphanRecovery()` | One orphan-recovery pass. |
| `tickWatchdog()` | One watchdog pass — force-reclaims in-flight entries past `handlerMaxRuntime`. |

### `RecordingOutboxListener`

Captures every one of the 26 `OutboxListener` callbacks into
`CopyOnWriteArrayList`s keyed by type. Snapshot any list to assert on
the full sequence of events the engine emitted:

```java
assertThat(outbox.recording().published()).hasSize(1);
assertThat(outbox.recording().retryScheduled())
    .singleElement()
    .satisfies(info -> assertThat(info.attempts()).isEqualTo(1));
```

See the [OutboxListener callback catalogue](OBSERVABILITY.md#outboxlistener-callback-catalogue)
for the full list of accessor methods.

### `EventAssertions` — AssertJ entry point

```java
import static io.github.bams22.outboxer.testkit.assertions.EventAssertions.assertThatStore;

assertThatStore(outbox.eventStore())
    .hasEvent(id)
    .withStatus(EventStatus.PROCESSING)
    .withAttempts(0)
    .withLastFailReason("transient");

assertThatStore(outbox.eventStore())
    .hasNoEvent(otherId)
    .hasTotalPending(0)
    .hasTotalDisabled(1);
```

## Three recipe patterns

### Recipe 1 — verify handler runs on Success

```java
@Test
void handlerRunsOnce(OutboxTestContext outbox) {
  AtomicInteger invocations = new AtomicInteger();
  OutboxTestContext ctx =
      OutboxTestContext.builder()
          .handler(simpleHandler(ORDER, p -> {      // ORDER = EventType.of("ORDER", OrderCreated.class)
            invocations.incrementAndGet();
            return EventOutcome.success();
          }))
          .build();

  UUID id = ctx.publisher().publish(ORDER, new OrderCreated("o-1"));
  ctx.manualEngine().tick();

  assertThat(invocations).hasValue(1);
  assertThatStore(ctx.eventStore()).hasNoEvent(id);
}
```

### Recipe 2 — verify retry-then-disable after N attempts

This exercises the `FailureHandler` chain end-to-end. The handler
always throws; after `max-attempts` the chain switches from `RetryAt`
to `Disable`. Advance the clock between ticks so each retry becomes
eligible.

```java
@Test
void disabledAfterThreeAttempts() {
  OutboxTestContext ctx =
      OutboxTestContext.builder()
          .defaultFailureHandler(
              FailureHandlers.<Object>builder()
                  .withMaxAttempts(3, MaxRetriesFailureHandler.ExhaustedAction.DISABLE)
                  .withFixedDelay(Duration.ofSeconds(1)))
          .handler(simpleHandler(FLAKY, p -> {      // FLAKY = EventType.of("FLAKY", String.class)
            throw new RuntimeException("nope");
          }))
          .clock(SettableClock.atEpoch())
          .build();

  UUID id = ctx.publisher().publish(FLAKY, "payload");

  for (int i = 0; i < 3; i++) {
    ctx.manualEngine().tick();              // attempt
    ctx.clock().advance(Duration.ofSeconds(2));
  }

  assertThatStore(ctx.eventStore())
      .hasEvent(id)
      .withStatus(EventStatus.DISABLED)
      .withAttempts(3);
  assertThat(ctx.recording().retryScheduled()).hasSize(2);
  assertThat(ctx.recording().disabled()).hasSize(1);
}
```

### Recipe 3 — verify watchdog force-reclaims a stuck handler

```java
@Test
void watchdogReclaimsStuckHandler() {
  EventTypeConfig fastDeadline =
      EventTypeConfig.defaults().toBuilder()
          .handlerMaxRuntime(Duration.ofSeconds(1))
          .build();

  // A handler that never returns — simulate with a latch in your real code.
  OutboxTestContext ctx =
      OutboxTestContext.builder()
          .defaultEventTypeConfig(fastDeadline)
          .handler(simpleHandler(SLOW, p -> EventOutcome.success()))
          .clock(SettableClock.atEpoch())
          .build();

  // Run tick() on a separate thread in your real test; here we simulate by
  // registering the in-flight record directly and advancing time past the deadline.
  ctx.clock().advance(Duration.ofSeconds(5));
  ctx.manualEngine().tickWatchdog();

  assertThat(ctx.recording().stuckReclaimed()).isNotEmpty();
}
```

In production tests the "stuck" handler is typically blocked on a
`CountDownLatch` acquired in another thread; the pattern above is the
declarative shape of the assertion.

## Interop with `@SpringBootTest`

`OutboxTestContext` is Spring-free. For integration tests that boot
the full Spring context (for example, to exercise
`@Transactional publish()`), use `@SpringBootTest` with
Testcontainers instead — the starter auto-configures the same
collaborators your application sees in production, and you assert via
the injected `OutboxEventPublisher`, `EventStore` etc. directly. See
`event-outboxer-spring-boot-starter/src/test/java/io/github/bams22/outboxer/spring/PostgresStarterIT.java`
for a worked example.

## Related documents

- [docs/OBSERVABILITY.md](OBSERVABILITY.md) — what the engine emits at
  runtime; test these signals by asserting on `recording()`.
- [docs/CONFIGURATION.md](CONFIGURATION.md) — property reference; every
  setter on the builder maps to a `event-outboxer.*` property.
