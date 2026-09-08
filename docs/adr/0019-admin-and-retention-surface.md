# ADR-0019: Admin and retention surface

## Status

Accepted — amended 2026-09-03 by
[ADR-0033](0033-archive-dedup-key-and-replay-from-archive.md): the
`OutboxAdmin` port grows `replayFromArchive` / `replayAllFromArchive`
(with the `ReplayOutcome` / `ReplayAllResult` types), exposed on both
admin surfaces (REST `POST /events/{id}/replay` +
`/events/replay-all`; Actuator `action=replay` on the two write
operations)

## Date

2026-07-26

## Context

The library shipped with no operational surface at all:

- `DISABLED` was a terminal state with no exit — no re-enable API
  anywhere; the only documented recovery was hand-written SQL against
  the library-owned schema, including a correct `version` bump.
- The only query the SPI offered was `EventStore.findById`.
- The archive table grew unboundedly (ADR-0008 explicitly deferred
  retention), and `DISABLED` rows accumulated in the hot `events`
  table forever, unindexed.
- `findByIdIncludingArchived`, specified in ADR-0008, was never
  implemented.

At the same time `EventStore` had grown to 13 methods across five
responsibilities; piling admin capabilities onto it would make every
third-party adapter implement operations the engine never calls.

## Decision

### 1. A dedicated `OutboxAdmin` SPI port

`event-outboxer-spi` gains `OutboxAdmin`, separate from `EventStore`:

- `findByStatus(status, eventType?, limit, cursor?)` — keyset
  pagination by `(created_at, id)` descending (`AdminCursor` record).
- `findInArchive(id)` — realizes ADR-0008's intent; returns the new
  `ArchivedEvent` domain record rather than an `Event` with a
  synthetic status (`EventStatus` stays untouched).
- `reenable(id)` — `DISABLED → PENDING` with `attempts = 0` (an
  operator re-enabling after a fix expects a fresh retry budget),
  `version++`, `run_at = now()`. Refuses non-`DISABLED` rows.
- `reenableAll(eventType, createdBefore?, limit)`.
- `purgeDisabled(eventType?, olderThan, limit)` — age approximated by
  `created_at`; the schema does not record the moment of disabling.
- `purgeArchive(archivedBefore, limit)`.

Implemented by `InMemoryOutboxAdmin` (no archive → empty/no-op) and
`PostgresOutboxAdmin`; behaviour pinned by
`AbstractOutboxAdminContractTest` run against both adapters. Migration
`V003__outbox_admin_index.sql` adds the partial index
`idx_events_disabled_created_at (created_at, id) WHERE status =
'DISABLED'` that serves all three DISABLED-scanning operations.

### 2. Two surface modules, activated by adding a dependency

- **`event-outboxer-admin-actuator`** — `@Endpoint(id = "outboxadmin")`
  with read/write/delete operations. Security and exposure follow the
  standard Actuator model; the endpoint is not exposed by default.
- **`event-outboxer-admin-rest`** — `@RestController` under a
  configurable base path. Strictly opt-in
  (`event-outboxer.admin.rest.enabled=false` by default). Guarded by
  `@PreAuthorize("hasAuthority(@outboxAdminRestProperties.getRequiredAuthority())")`
  — the permit name comes from
  `event-outboxer.admin.rest.required-authority`, not from code.

Security posture of the REST module:

- No Spring Security on the classpath → the annotation is inert and
  the API runs open. Accepted trade-off for security-less apps.
- Spring Security present but `@EnableMethodSecurity` absent →
  `@PreAuthorize` would be **silently ignored**. The auto-configuration
  fail-fasts at startup (detecting the
  `preAuthorizeAuthorizationMethodInterceptor` bean by name — the
  interceptor beans are typed by interface, so name detection is the
  reliable option). `event-outboxer.admin.rest.enforce-authority=false`
  is the explicit opt-out.

This amends CLAUDE.md invariant 9: Spring now appears in the starter
AND in the two admin surface modules — they are Spring-integration
modules by nature. Core and storage/lock adapters remain Spring-free,
and the surface modules are banned from depending on core.

### 3. Shipped retention, off by default

`RetentionTask` runs on the existing `MaintenanceScheduler`, looping
in `batch-size`-bounded DELETEs until a batch comes back short.
Configured via `event-outboxer.retention.*`
(`archive-older-than`, `disabled-older-than`, `batch-size: 1000`,
`interval: 1h`); both thresholds default to null = off — deleting data
is never a surprise default, but enabling it is one YAML line instead
of a bespoke user-written job.

## Consequences

- The 3am recovery story becomes an API call (or curl) instead of
  hand-written UPDATEs against internal schema.
- `EventStore` stays engine-only; adapter authors implement admin
  capabilities separately and optionally.
- Two more modules (15 total); both surface modules are optional and
  version-managed by the BOM.
- `attempts = 0` on re-enable means a re-enabled poison event gets a
  full fresh retry budget before disabling again — deliberate.
- (2026-09-07) `PostgresOutboxAdmin` takes the same `MetricsSnapshotCache`
  as the store and invalidates it after every mutation that changed rows,
  so a re-enable, purge or replay is visible in backlog gauges and health
  on the next read, not after `metrics-cache-ttl`; `purgeArchive` does
  not, the snapshot does not cover the archive. A bulk sweep invalidates
  once per batch rather than once per `RetentionTask` pass: each batch
  really does change the counts, and the adapter cannot see the pass.
  The two adapters also share their column lists and row mappers
  (`EventRows`, internal) so a column added to the store reaches the
  admin's statements by construction. Neither changes the port — the
  freshness is a property of this adapter pair, and the in-memory admin
  makes no such promise.
- (2026-09-07) Dropping the cached entry does not by itself make the next
  read fresh, and the first cut of the change above did not: a snapshot
  computation already in flight finishes after the invalidation and puts
  its pre-mutation counts back, restoring exactly the TTL-long staleness
  the invalidation was meant to remove; and inside a caller transaction
  the invalidation ran at statement time while the rows became visible
  only at commit. Two additions close both windows.
  `MetricsSnapshotCache.put` is now conditional — a snapshot whose
  `takenAt` predates the last `invalidate()` is refused, enforced in the
  in-memory flavour by a second reference and in the Redis flavour by a
  Lua compare-and-set against an `<prefix>invalidated-at` key, which also
  gives the barrier a Clock so both sides of the comparison come from the
  application's clock. And the starter wraps the cache handed to the admin
  bean in `TransactionAwareMetricsCacheInvalidation`, which defers the
  invalidation to after-commit when a transaction is active and drops it
  on rollback. What remains, and is documented rather than fixed: the
  Redis barrier compares millisecond timestamps stamped by different pods,
  so it is only as good as their clock sync; the cost of losing that race
  is one TTL of stale gauges, never a wrong number.
- **Deferred, restated 2026-09-08 and deliberately not in 0.8.0:
  `onEventReenabled`.** No `OutboxAdmin` mutation fires an
  `OutboxListener` callback — `reenable`, `reenableAll`,
  `replayFromArchive`, `replayAllFromArchive`, `purgeDisabled` and
  `purgeArchive` change rows and return counts, and neither the
  adapters nor the two surface modules hold a listener. The consequence
  accepted for now: a fleet cannot tell an operator-driven re-enable
  from an ordinary publish. The row reappears as `PENDING` and is
  claimed like any other, so every consume-side callback fires for its
  second lifecycle; the backlog gauges move promptly (the 2026-09-07
  invalidation above), but nothing marks the moment or names the actor,
  and a dashboard reads the drop in the `DISABLED` gauge as if the
  engine had done it. Until the callback exists, the audit trail of an
  admin action is the surface it came through — the Actuator endpoint
  on the management port, the REST controller behind its `@PreAuthorize`
  authority — not the outbox's event bus.
  Why it waits instead of shipping with the rest of this ADR: a
  callback for `reenable` alone would be the wrong shape. A mutation
  bus wants every mutation under one naming scheme (single and bulk
  re-enable, single and bulk replay, both purges, each with its
  counts), and it would put a listener into the `OutboxAdmin` port,
  which today has none — the listener lives in the engine, and the
  admin adapters are reached directly by the surfaces. Revisit it
  together with the split of `OutboxListener` (30 callbacks on one
  interface, ADR-0013 §Negative consequences) and add the whole
  mutation set at once.
- Deferred without a plan: a WebFlux variant of the REST module, any
  dashboard/UI.

## Related decisions

- [ADR-0008](0008-three-statuses-plus-optional-archive.md) — archive
  model; amended by this ADR (retention + `findInArchive` now exist).
- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — port design;
  `OutboxAdmin` keeps `EventStore` from becoming a god-interface.
- [ADR-0016](0016-maven-module-structure.md) — module layout (15 when
  this ADR was written; 21 published modules today).
