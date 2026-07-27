# ADR-0019: Admin and retention surface

## Status

Accepted

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
- Deferred: `onEventReenabled` listener event (planned together with
  the OutboxListener split), a WebFlux variant of the REST module,
  any dashboard/UI.

## Related decisions

- [ADR-0008](0008-three-statuses-plus-optional-archive.md) — archive
  model; amended by this ADR (retention + `findInArchive` now exist).
- [ADR-0010](0010-storage-agnostic-core-via-spi.md) — port design;
  `OutboxAdmin` keeps `EventStore` from becoming a god-interface.
- [ADR-0016](0016-maven-module-structure.md) — module layout (now 15).
