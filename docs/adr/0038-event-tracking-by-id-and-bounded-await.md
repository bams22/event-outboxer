# ADR-0038: Tracking an event by id — status lookup and bounded await, no request-reply

## Status

Accepted — 2026-10-06. Design agreed and implemented the same day
(ADR, API + core, starter + testkit + PostgreSQL integration test,
docs). One detail settled during implementation: the two exceptions sit
under a new `TrackingException` category (codes `OUTBOX-5xx`), matching
the two-level hierarchy of the other categories.

Clarified 2026-10-06 (review): the first draft recommended calling
`await` from an `afterCommit` synchronisation. With the starter that
throws — Spring still reports the transaction as active inside its
synchronisation callbacks — and it buys nothing over awaiting in the
caller once the transactional method returns. The guard is kept as is;
§4 and the Consequences now recommend the caller instead.

Fixed 2026-10-06 (review): the first wiring passed the `OutboxAdmin`
to the tracker whenever one existed, so with the archive off and its
optional table absent every no-row lookup failed. Archive lookups are
now gated on the archive setting (§5).

## Date

2026-10-06

## Context

Application code that publishes an event sometimes needs to know what
happened to it afterwards. Three distinct asks arrive under one
sentence — "publish, get an object back, use it to check or wait":

1. **Status** — is the event still waiting, in flight, failed for good,
   or done?
2. **Await** — block for a bounded time until it is done or failed.
3. **Result** — obtain a value the handler produced.

What the library offers today:

- `OutboxEventPublisher.publish(...)` returns the event's `UUID`
  (ADR-0031); the same value is visible as `Event.id()` and
  `EventContext.eventId()` inside the handler.
- `EventStore.findById(id)` reads the hot table (`PENDING` /
  `PROCESSING` / `DISABLED`); `OutboxAdmin.findInArchive(id)` reads the
  opt-in archive (ADR-0008, ADR-0019). The Actuator and REST admin
  surfaces already combine the two per id — for operators, not for
  application code, and behind the admin security posture.
- `OutboxListener` fires `onEventProcessed` / `onEventDisabled` /
  `onEventSkipped` — but only in the JVM that finalised the event.
- Core has a `TransactionContext` port (`isActive()`, `afterCommit()`),
  wired by the starter to Spring's
  `TransactionSynchronizationManager`. The publisher uses it for the
  `no-transaction-policy` guard and for waking the local poller only
  after commit.

Forces that shape the answer:

- **ADR-0002** — `publish()` participates in the caller's transaction.
  The row becomes visible to the engine only after commit. Anything
  that waits for processing *inside* that transaction waits for its
  own commit and can only time out.
- **ADR-0001 / ADR-0029** — the service runs as replicas, and a
  publish-only instance never processes anything. The JVM that
  published is in general *not* the JVM that processes. A completion
  signal therefore cannot be an in-process callback; the database is
  the only shared truth.
- **ADR-0008** — success means `DELETE` from the hot table; the archive
  is opt-in. Without the archive, "the row is gone" is the only trace
  of success, and it is indistinguishable from "this id never
  existed" or "the publishing transaction rolled back". `Skip`
  outcomes route through `markProcessed` as well, so with the archive
  on they are archived like successes.
- **ADR-0015** — at-least-once: a handler may run twice for one event.
  "Done" can only mean "the engine finalised it after at least one
  successful run". A per-run *result value* has no single owner.
- **ADR-0006** and the lease-locker experiment — LISTEN/NOTIFY was
  measured and removed; it serialises commits and does not survive
  pgBouncer transaction pooling. Push-style completion signals through
  PostgreSQL are off the table.
- The adaptive poller already wakes the local instance's poller right
  after commit (`afterCommit`) and otherwise runs on a 500 ms – 10 s
  cadence (`poll-min-interval` / `poll-max-interval`). Whatever the
  caller does, processing latency is set by the engine, not by the
  waiter.

## Alternatives considered

- **A. `publish()` returns a handle object with `await()`** (a
  `Future`-like type instead of `UUID`). Pros: matches the intuitive
  phrasing of the ask. Cons: a breaking change of the one method every
  user calls; the handle is an in-memory object while the natural
  thing to pass between requests, threads and processes is the id;
  the obvious misuse — `publish(...).await()` on the same line, inside
  the transaction — hangs until timeout; with replicas the engine
  cannot complete a local future anyway, so the object would just be
  a poller in disguise. Rejected.
- **B. Store the handler's return value** (`EventType<T, R>`, a result
  column or table, a typed read API). Cons: the hot table deletes
  successful rows by design (ADR-0008), so results need their own
  table, retention and serialization format; at-least-once means two
  runs may produce two different results and the library cannot pick
  the right one; this is the feature set of a job scheduler with
  return values, which CLAUDE.md puts out of scope, and neither
  jobrunr nor db-scheduler offers it. The same guarantee is available
  today for free: the handler writes its outcome into the
  application's own table inside the handler's transaction, keyed by
  `EventContext.eventId()`. Rejected.
- **C. Completion through `OutboxListener` callbacks.** Works only when
  *this* JVM finalised the event; on a publish-only node it never
  fires. Rejected as the mechanism; kept as a possible later
  optimisation on top of the chosen one (see Deferred).
- **D. LISTEN/NOTIFY on finalise.** Rejected for the reasons recorded
  in ADR-0006 and the lease-locker measurements.
- **E. A read-only tracker over the existing lookups** — status by id
  and a bounded, DB-polling await by id, with the `UUID` from
  `publish()` acting as the handle. Chosen.

## Decision

### 1. The id is the handle

`OutboxEventPublisher.publish(...)` keeps returning `UUID`. That value
is serialisable, survives request boundaries and process restarts, and
is already the key of every lookup the library offers. No new return
type, no breaking change.

### 2. A read-only port: `OutboxEventTracker`

`event-outboxer-api` gains the package
`io.github.bams22.outboxer.api.track`:

```java
public interface OutboxEventTracker {

    TrackedState state(UUID eventId);

    AwaitResult await(UUID eventId, Duration timeout);

    AwaitResult await(UUID eventId, Duration timeout, Duration pollInterval);
}

public enum TrackedState { PENDING, PROCESSING, DISABLED, ARCHIVED, ABSENT }

public sealed interface AwaitResult
        permits AwaitResult.Completed, AwaitResult.Disabled, AwaitResult.TimedOut {

    record Completed(UUID eventId, @Nullable ArchivedEvent archived) implements AwaitResult {}

    record Disabled(Event event) implements AwaitResult {}

    record TimedOut(UUID eventId, TrackedState lastSeen, Duration waited) implements AwaitResult {}
}
```

`TrackedState` is deliberately a different type from the persisted
`EventStatus`: the three hot-table statuses plus the two things an
observer can say about an id that is *not* in the hot table.

### 3. `state(id)` — honest, stateless lookup

| Lookup result | `TrackedState` |
|---|---|
| hot-table row, `status = PENDING` | `PENDING` |
| hot-table row, `status = PROCESSING` | `PROCESSING` |
| hot-table row, `status = DISABLED` | `DISABLED` |
| no hot-table row, archive row present | `ARCHIVED` |
| no hot-table row, no archive row | `ABSENT` |

`ABSENT` is documented as *exactly* what it is: processed without the
archive enabled, never committed, purged by retention, or simply an
unknown id. The library does not know which, and `state()` does not
pretend to. With `event-outboxer.storage.archive-enabled=true` every
successful or skipped event resolves to `ARCHIVED`, which is the
configuration to recommend to anyone who needs an authoritative
"done" for arbitrary ids.

### 4. `await(id, timeout[, pollInterval])` — bounded, DB-polled

Precondition, stated in the Javadoc and enforced where possible: the
publishing transaction has committed. The method:

1. **Fails fast inside a transaction.** If
   `TransactionContext.isActive()` is `true` on the calling thread, it
   throws `AwaitInTransactionException` immediately instead of running
   into the timeout. In the starter this is Spring's actual-transaction
   check, the same signal the `no-transaction-policy` guard uses. In
   plain-Java setups with no observable transaction manager the guard
   is off (default `TransactionContext.neverActive()` for the tracker;
   note the deliberate asymmetry with the publisher's
   `alwaysActive()` default — each default is the one that does not
   break the common case for that component). Transaction
   synchronisation callbacks (`afterCommit`, `afterCompletion`) count
   as inside: Spring clears the actual-transaction flag only after
   they run, with the connection still bound to the thread. That is
   kept on purpose — waiting there would hold the transaction's
   connection, surface lookup failures from an already-committed
   transaction, and, under an outer `REQUIRED` transaction, move the
   wait to that transaction's commit. The supported shape is: return
   the id from the transactional method, `await` in its
   non-transactional caller.
2. **Polls the row by primary key** until the deadline:
   `PENDING` / `PROCESSING` → sleep `min(pollInterval, remaining)` and
   look again; `DISABLED` → return `Disabled(event)` (carrying
   `attempts` and `lastFailReason`); no row → consult the archive once
   and return `Completed(archived-or-null)`.
3. **Returns `TimedOut(lastSeen, waited)`** when the deadline passes.
   The caller can tell "not yet" from "broken".
4. **Never blocks without a bound.** `timeout` is required and
   positive; `pollInterval` is positive; the default interval is
   200 ms (`event-outboxer.tracker.poll-interval`). The library sets
   no upper cap on `timeout`, but the Javadoc says seconds, not
   minutes: every waiter is a parked thread plus one primary-key query
   per interval.
5. **Interrupt** restores the interrupt flag and throws
   `AwaitInterruptedException` (unchecked, like every other
   `OutboxException`).

`Completed` means "the outbox has finalised this event after at least
one `Success` or `Skip`" (ADR-0015). One consequence of the handle
contract must be written down rather than hidden: an id whose
publishing transaction rolled back is also "not in the hot table" and
is reported `Completed` with `archived == null`. The caller owns that
transaction and knows whether it committed; `await` is the handle for
ids the caller committed, `state()` is the query for ids of unknown
provenance.

### 5. Where it lives

- `event-outboxer-core`: `core.track.DefaultOutboxEventTracker`,
  `@Builder` on a private validating constructor — required `store`
  (`EventStore`); optional `admin` (`OutboxAdmin`, `null` → the archive
  is never consulted and the no-row case maps to `ABSENT` /
  `Completed(null)`), `transactionContext` (default `neverActive()`),
  `pollInterval` (default 200 ms). No Spring, no engine lifecycle: the
  tracker is a stateless reader and works on publish-only nodes.
- `OutboxEngine.tracker()` next to `publisher()` for plain-Java users;
  `OutboxEngineBuilder` resolves the `TransactionContext` default per
  consumer (publisher → `alwaysActive()`, tracker → `neverActive()`)
  when the user did not supply one, and hands the tracker its `admin`
  only with `archiveEnabled(true)` (default `false`).
- `event-outboxer-spring-boot-starter`: an `OutboxEventTracker` bean
  (`@ConditionalOnMissingBean`), present in both roles, built from the
  `EventStore`, the `OutboxAdmin` bean when
  `event-outboxer.storage.archive-enabled=true`, the starter's
  `TransactionContext` and `event-outboxer.tracker.poll-interval`.
- The archive is consulted only when it is enabled. Its table is
  optional DDL (STORAGE.md): an application that manages the schema
  itself may never have created it, and an `OutboxAdmin` bean exists
  regardless — so "admin port present" is not "archive present".
- `event-outboxer-testkit`: `OutboxTestContext.tracker()`.
- Admin surfaces are unchanged; they already perform the combined
  per-id lookup for operators.

### 6. What this ADR explicitly does not add

- No result values. The supported pattern is documented as a recipe:
  the handler persists its outcome in the application's own table
  inside the handler's transaction, keyed by `EventContext.eventId()`;
  `Completed` is the signal to read it.
- No `Future` from `publish()`, no completion callbacks for
  application code, no cross-service awaiting. This ADR draws the
  "not request-reply" line next to ADR-0001's "not cross-service"
  line.

### Deferred — build only if measured to matter

An in-JVM fast path: the tracker subscribes to the engine's listener
registry and wakes a waiter early when *this* instance finalised the
id. The database stays the truth (a wake triggers a re-read, never a
direct answer). It helps single-instance deployments and tests; it
does nothing for publish-only nodes. Per the project's
measure-then-decide rule it is added only if the 200 ms poll proves
inadequate in practice.

## Rationale

The decisive observation is that the only shared truth between the
publishing replica and the processing replica is the database row.
Every design that promised more — a future, a callback, a NOTIFY —
either lied on publish-only nodes or re-introduced a mechanism the
project has already measured and removed. Polling a primary key at a
sub-second interval for a bounded time is cheap, works behind
pgBouncer, and needs no new table, column or migration.

Keeping `publish()` on `UUID` and putting the read side in its own
port mirrors how the project already separates writing
(`OutboxEventPublisher`) from administration (`OutboxAdmin`): small,
single-purpose interfaces over the same SPI. Two vocabularies —
`EventStatus` for what is persisted, `TrackedState` for what an
observer can know — keep the archive-off ambiguity visible instead of
papering over it with a `COMPLETED` the library cannot vouch for.

The transaction guard turns the one realistic misuse from a silent
timeout into an immediate, named exception, using a port that already
exists.

## Consequences

### Users of the library

- One new, additive API package and one new bean. Existing code is
  untouched; `publish()` keeps its signature.
- `await` is for use **after commit**: in the non-transactional caller
  of the `@Transactional` method that published, in a later request,
  or on any other code path outside a transaction. Inside
  `@Transactional` — including its `afterCommit` / `afterCompletion`
  synchronisations — it throws.
- Processing latency is unchanged: an idle instance picks the event
  up on its poller's cadence (immediately on the publishing instance
  thanks to the after-commit wake, up to 10 s elsewhere). Waiting does
  not make the engine faster.
- Anyone who needs an authoritative "done" for arbitrary ids turns on
  the archive; without it `ABSENT` is the honest answer.
- Results travel through the application's own tables, never through
  the outbox.

### Library maintainers

- `TrackedState` and `AwaitResult` are public API under japicmp; both
  are additive in this release. Any future change to finalisation
  (new statuses, a different success path) must revisit the mapping
  table in §3.
- `DefaultOutboxEventTracker` is core code over SPI ports and is
  tested in core against the in-memory adapter, and end-to-end in the
  starter's PostgreSQL integration tests (archive on and off, DISABLED
  path, in-transaction guard). Adapters do not learn about it
  (invariant 2 holds).
- The `TransactionContext` default asymmetry (publisher
  `alwaysActive`, tracker `neverActive`) is intentional and must stay
  documented on both constructors.

### Operations

- Each concurrent waiter costs one parked thread and one primary-key
  lookup per `poll-interval`. On a virtual-thread handler executor the
  thread is free; the query is not. A burst of waiters on an API node
  is ordinary read load on the hot table, visible as such.
- Nothing new to deploy: no migration, no table, no extra connection.

## Related decisions

- [ADR-0001](0001-local-embedded-outbox-scope.md) — scope boundary;
  this ADR adds "not request-reply" beside "not cross-service".
- [ADR-0002](0002-participate-in-client-transaction.md) — why awaiting
  inside the publishing transaction can only time out, and why the
  guard exists.
- [ADR-0006](0006-no-listen-notify-in-mvp.md) — push-style completion
  through PostgreSQL rejected; polling by primary key chosen instead.
- [ADR-0008](0008-three-statuses-plus-optional-archive.md) — success
  is `DELETE`; the archive is what makes `ARCHIVED` possible.
- [ADR-0015](0015-at-least-once-semantics.md) — why "done" is a
  finalisation fact, not a result value.
- [ADR-0019](0019-admin-and-retention-surface.md) — `OutboxAdmin`
  supplies the archive lookup the tracker reuses.
- [ADR-0029](0029-publish-only-is-explicit.md) — publish-only nodes
  are the topology that rules out in-process completion signals.
- [ADR-0031](0031-typed-event-key.md) — `publish()` returns the id that
  this ADR turns into the handle.
