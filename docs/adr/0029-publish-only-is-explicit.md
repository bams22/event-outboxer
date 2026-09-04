# ADR-0029: Publish-only mode is an explicit opt-in

## Status

Accepted.

## Date

2026-08-28

## Context

`OutboxEngineBuilder.build()` rejected an empty handler set ("at least
one EventHandler must be registered"). Two legitimate deployments hit
that wall: a service that only *emits* events for a sibling deployment
to process, and one code base deployed as API nodes (publish) and
worker nodes (process). Simply dropping the check would make the other,
far more common case silent: a Spring Boot application whose handlers
were not picked up (outside component scan, wrong profile, missing
`@Component`) would boot cleanly and pile up `PENDING` events that
nobody ever processes.

## Alternatives considered

- **Always allow an empty handler set, log a warning.** Zero
  configuration for the legitimate cases, but a warning in the startup
  log is exactly what gets missed when the wiring is wrong — and the
  failure shows up days later as a growing backlog.
- **Keep the hard requirement.** Publish-only deployments would need a
  dummy handler, which is a lie in the code and still starts a poller.
- **Explicit flag** — chosen.

## Decision

- `OutboxEngineBuilder.publishOnly(boolean)` (default `false`) and the
  starter property `event-outboxer.publish-only` (default `false`).
- With the flag the engine registers its worker, runs heartbeat /
  orphan recovery / retention / stale-claim sweep and exposes the
  publisher, but creates **no executors and no pollers**; handlers
  registered on it are ignored (logged at INFO with their count), so
  the same application can be deployed in both roles by configuration.
- Without the flag an empty handler set is rejected with the dedicated
  `NoEventHandlersException` (a `ConfigurationException`); the starter
  maps it to a `FailureAnalyzer` diagnosis that names the handler
  contract and the property.

## Rationale

Intent stated once in configuration beats a heuristic. The flag makes
the publish-only role visible where deployments are described
(`application-api.yml` vs `application-worker.yml`), while the default
keeps the safety net for the accidental case — with a startup failure
that explains itself instead of a log line.

## Consequences

- **Users**: publish-only deployments set one property; everyone else
  is unaffected. Handler beans on a publish-only instance are inert,
  which is the intended way to share a code base between roles.
- **Maintainers**: the poller/executor assembly is skipped entirely in
  publish-only mode; maintenance tasks must keep working with zero
  pollers (the health-check task already does).
- **Operations**: a publish-only worker still appears in
  `event_outboxer.workers` and in the health endpoint; per-type backlog
  gauges are registered only for the handlers a polling instance runs.

## Amendment (2026-09-04): no derived stale-claim threshold without handlers

The Decision above lists the stale-claim sweep among the maintenance a
publish-only engine keeps running. Its threshold, when not configured,
is derived as 2 × the largest `handler-max-runtime` of the types the
instance polls — and a publish-only instance polls none, so the
derivation produced **zero**: every `stale-claim-sweep-interval` the
publish-only instance returned *every* `PROCESSING` row of the whole
fleet to `PENDING` with `attempts + 1`, including rows sitting in live
workers' executor queues. Each of those was then handled by its
original claimer from its in-memory queue *and* by whoever re-claimed
it: duplicates by the hundreds and an attempts budget burning down on
a 5-minute cadence. The benchmark harness (ADR-0034) found it through
its `crash` preset, whose driver holds a publish-only context: 390
duplicates on 5 000 events, 186 of them far from the kill.

**Change.** `OutboxEngineBuilder` resolves the threshold to *none* when
the instance polls no event type and no explicit
`maintenance.stale-claim-threshold` is set; the sweeper is then not
scheduled at all and startup logs why. With an explicit threshold a
publish-only instance sweeps with it — an operator who wants the
safety net to live on the API tier can still have it, with a value
chosen for the fleet. Polling instances are unaffected: they derive
from their own handlers as before.

**Consequence for ADR-0029's Decision.** Read "stale-claim sweep" in the
maintenance list as "stale-claim sweep, when it has a threshold".

## Related decisions

- [ADR-0004](0004-per-event-type-worker-isolation.md) — one poller per
  handled event type; publish-only is the zero-poller degenerate case.
- [ADR-0020](0020-no-inmemory-storage-in-production.md) — the same
  "explicit over silent" stance for storage.
