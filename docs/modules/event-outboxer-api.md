# event-outboxer-api

The public API — the only module application code should program
against. Publisher, handler and listener contracts, the failure-handler
chain, domain value objects and the exception hierarchy. No Spring, no
Jackson, no JDBC; the leaf of the dependency graph.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-api` |
| Java packages | `io.github.bams22.outboxer.api.*`, `…outboxer.domain.*`, `…outboxer.domain.exception.*` |
| Depends on | `slf4j-api`, `jspecify` (compile-scope, transitive by design — ADR-0018) |
| Who adds it | Usually nobody directly — it arrives transitively via the starter or core |

## Why it exists

Splitting the contracts from the engine keeps the surface applications
compile against small and stable: your handlers and publishers depend
on interfaces and records only, while the engine
([core](event-outboxer-core.md)) and adapters evolve behind them.
Every public package is `@NullMarked` (JSpecify), so downstream
nullness checkers work out of the box.

## What it contains

### Publishing (`api.publish`)

- **`OutboxEventPublisher`** — the entry point you inject and call:
  - `UUID publish(String eventType, Object payload)`
  - `UUID publish(String eventType, Object payload, Instant runAt)` — delayed events
  - `UUID publish(String eventType, Object payload, PublishOptions options)`
  - `List<UUID> publishAll(Collection<PublishRequest> requests)` — fail-fast,
    all-or-nothing; result ids match request order.

  Contract: `publish` **participates in the caller's current
  transaction** ([ADR-0002](../adr/0002-participate-in-client-transaction.md))
  — rollback means the event is never persisted. Outside a transaction
  the `no-transaction-policy` applies (`FAIL` throws
  `NoTransactionException`; `IGNORE` writes anyway — tests only).
- **`PublishOptions`** (record + builder) — optional per-call tuning:
  `runAt` (delay), `priority`, `traceContext` (explicit override),
  `dedupKey`. A `dedupKey` gives *work coalescing*: at most one
  `PENDING` event per `(eventType, dedupKey)`; a coalesced publish
  returns the existing event's id
  ([ADR-0021](../adr/0021-dedup-key-single-inflight-per-key.md)).
  This is **not** exactly-once — handlers stay idempotent. There is
  deliberately no `lockKey` option: lock keys are derived at handle
  time by the handler ([ADR-0012](../adr/0012-extract-lock-key-on-handler.md)).
- **`PublishRequest`** (record + builder) — one entry of a `publishAll` batch.

### Handling (`api.handle`)

- **`EventHandler<T>`** — the main interface you implement:

  ```java
  @Component
  public class SendEmailHandler implements EventHandler<SendEmailPayload> {
      @Override public String eventType() { return "SEND_EMAIL"; }
      @Override public Class<SendEmailPayload> payloadType() { return SendEmailPayload.class; }

      @Override
      public EventOutcome handle(EventContext ctx, SendEmailPayload payload) {
          mailer.send(payload.email());
          return EventOutcome.Success.INSTANCE;
      }

      @Override
      public String extractLockKey(SendEmailPayload p) {   // optional, default null
          return "customer:" + p.email();                  // serialize per business key
      }
  }
  ```

  Contracts: `eventType()` is a stable natural key in the database —
  never rename it once events exist. **Handlers must be idempotent**
  (at-least-once, [ADR-0015](../adr/0015-at-least-once-semantics.md)).
  An uncaught exception is treated as
  `EventOutcome.Retry(e.getMessage(), null, e)` — no need to wrap
  everything in try/catch. `failureHandler()` (default `null`) lets a
  handler carry its own retry policy, winning over per-type and global
  configuration.
- **`EventContext`** (record) — metadata passed to `handle`: `eventId`,
  `eventType`, `attempt` (1-based), `createdAt`, `claimedAt`,
  `workerId`, `traceContext`. Never carries the payload.
- **`EventOutcome`** (sealed) — exactly four results:

  | Outcome | Meaning |
  |---|---|
  | `Success.INSTANCE` | done; event is deleted (or archived) |
  | `Retry(reason, delayOverride, cause)` | transient failure; chain (or the override) picks the delay |
  | `Fail(reason, cause)` | permanent failure; straight to `DISABLED`, no retries |
  | `Skip(reason)` | successful no-op (idempotency short-circuit); stored like success, reported separately |

- **`FailureHandler<T>`** / **`FailureContext<T>`** / **`FailureDecision`**
  — the chain-of-responsibility that turns a failure into
  `RetryAt` / `Disable` / `Delete`
  ([ADR-0007](../adr/0007-failure-handler-chain-of-responsibility.md)).
  Resolution order: handler's own → per-type → global default.
  Implementations must be thread-safe and must *not* fire listener
  callbacks — the engine emits them after the decision is persisted.

### Built-in failure handlers (`api.handle.builtin`)

`FailureHandlers.defaults()` =
`Log(WARN) → MaxRetries(10, DISABLE) → ExponentialBackoff(5s, ×2.0, cap 1h, jitter 0.2)`.
Build custom chains fluently:

```java
FailureHandler<PaymentPayload> fh = FailureHandlers.<PaymentPayload>builder()
    .withLogging(Level.WARN)
    .withMaxAttempts(5, MaxRetriesFailureHandler.ExhaustedAction.DISABLE)
    .withExponentialBackoff(Duration.ofSeconds(30), 2.0, Duration.ofHours(2), 0.2)
    .build();   // terminators: withExponentialBackoff / withFixedDelay / withNoRetry
```

Notes: an explicit `EventOutcome.Fail` bypasses the retry budget and
finalizes immediately; `Retry.delayOverride` wins over any computed
backoff; `NoRetryFailureHandler` disables on first failure (useful for
validation-style handlers).

### Observability (`api.observer`)

**`OutboxListener`** — 21 callbacks, all default no-ops, each with its
own immutable `*Info` record: event lifecycle (`onEventPublished`,
`onEventClaimed`, `onEventProcessed`, `onEventRetryScheduled`,
`onEventDisabled`, `onEventDeleted`, `onEventSkipped`), failures
(`onHandlerError`, `onUnknownEventType`, `onEventSerializationError`,
`onStorageError`, `onDispatchRejected`), locking
(`onLockAcquisitionFailed`, `onLockReleaseFailed`), worker lifecycle
(`onWorkerRegistered`, `onWorkerGracefulStop`, `onWorkerDeregistered`,
`onHeartbeatFailed`), recovery (`onOrphansReclaimed`,
`onStuckHandlerReclaimed`) and `onEngineCrashed` (fatal — the engine
does not self-recover).

Contracts: callbacks run on poller / worker / maintenance threads —
implementations must be thread-safe and fast (offload long work); the
engine isolates listener exceptions, they never affect event
processing. The full callback catalogue with payloads is in
[OBSERVABILITY.md](../OBSERVABILITY.md#outboxlistener-callback-catalogue).

### Domain model (`domain`)

| Type | Role |
|---|---|
| `Event` | full read projection (admin/API views), incl. `status`, `attempts`, `version`, `dedupKey` |
| `PendingEvent` | what the publisher writes — payload already serialized, `payloadFormat` recorded |
| `ClaimedEvent` | what the dispatcher processes; carries `claimedVersion` for optimistic locking ([ADR-0014](../adr/0014-optimistic-locking-via-version-field.md)) |
| `ArchivedEvent` | row in the opt-in archive ([ADR-0008](../adr/0008-three-statuses-plus-optional-archive.md)) |
| `EventStatus` | `PENDING` / `PROCESSING` / `DISABLED` |
| `SerializedPayload` | text *or* bytes lane, exactly one set ([ADR-0025](../adr/0025-binary-capable-serializer-spi-and-payload-format.md)); `toString()` never leaks payload content |
| `WorkerId`, `WorkerInfo` | worker identity (`{hostname}-{pid}-{uuid8}`) and registration metadata |

`payloadClass` on the event records is diagnostics only — the
deserialization target is always `EventHandler.payloadType()`.

### Exceptions (`domain.exception`)

All unchecked, rooted at `OutboxException`; operational exceptions
carry an `OUTBOX-XXX` code constant:

| Branch | Codes | Notes |
|---|---|---|
| `PublishException` → `PublishValidationException`, `PublishSerializationException`, `NoTransactionException`, `PublishFailedException` | 101–104 | what `publish()` can throw; catching `PublishException` is enough to roll back |
| `HandleException` → `UnknownEventTypeException`, `PayloadDeserializationException`, `UnknownPayloadFormatException` | 201–203 | engine-side; never thrown back to application code — routed through the failure chain |
| `StorageException` → `EventStoreException`, `WorkerRegistryException` | 302–303 | adapters must wrap native errors into these; OCC conflicts are `false` returns, not exceptions |
| `LockException` → `LockAcquisitionException`, `LockReleaseException` | 401–402 | "busy" is `Optional.empty()`, not an exception |
| `ConfigurationException` → `DuplicateHandlerException`, `InvalidEventTypeConfigException`, `InvariantViolationException` | — | fail-fast at startup |
| `EngineLifecycleException` → `EngineNotStartedException` | — | lifecycle misuse |

## When to use it

You consume it, always — through the starter or core. Add it
*directly* only if a module of yours (say, a shared `contracts` jar
holding payload DTOs and `EventHandler` implementations) must compile
against the outbox API without dragging the engine in.

## How to use it well

- Payloads are **explicit DTOs** — records with stable field names;
  no lambdas, no method references
  ([ADR-0003](../adr/0003-explicit-dto-payload.md)). Give new fields
  defaults, use `@JsonAlias` for renames, never change a field's type
  (publish a new event type instead) — see
  [CONFIGURATION.md §DTO evolution](../CONFIGURATION.md#dto-evolution-and-rolling-deploys).
- Return `Fail` for permanent errors (a 404 that will never heal),
  `Retry` for transient ones, `Skip` when idempotency detects the work
  is already done — the distinction feeds separate metrics.
- Use `extractLockKey` when events for the same business entity must
  not run concurrently across the fleet; requires configuring an
  [`EntityLocker`](../CONFIGURATION.md#event-outboxerlock) (default `noop`).

## Related

- [SPI module](event-outboxer-spi.md) — the adapter-facing ports.
- [OBSERVABILITY.md](../OBSERVABILITY.md) — listener callback catalogue.
- ADRs: [0003](../adr/0003-explicit-dto-payload.md), [0007](../adr/0007-failure-handler-chain-of-responsibility.md), [0012](../adr/0012-extract-lock-key-on-handler.md), [0013](../adr/0013-outbox-listener-for-observability.md), [0015](../adr/0015-at-least-once-semantics.md), [0018](../adr/0018-jspecify-for-nullness.md), [0021](../adr/0021-dedup-key-single-inflight-per-key.md).
