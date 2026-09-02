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

## Related decisions

- [ADR-0004](0004-per-event-type-worker-isolation.md) — one poller per
  handled event type; publish-only is the zero-poller degenerate case.
- [ADR-0020](0020-no-inmemory-storage-in-production.md) — the same
  "explicit over silent" stance for storage.
