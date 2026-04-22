# Observability

How to tell what event-outboxer is doing from the outside: the
Actuator health indicator, the Micrometer metrics catalogue, the
`OutboxListener` callback reference, and a short troubleshooting
playbook for the five most common production scenarios.

## Contents

1. [Health indicator](#health-indicator)
2. [Kubernetes probes](#kubernetes-probes)
3. [Micrometer metrics reference](#micrometer-metrics-reference)
4. [OutboxListener callback catalogue](#outboxlistener-callback-catalogue)
5. [Troubleshooting playbook](#troubleshooting-playbook)

---

## Health indicator

When [Spring Boot Actuator](https://docs.spring.io/spring-boot/reference/actuator/endpoints.html)
is on the classpath, the starter auto-registers an `outboxHealthIndicator`
bean. The health fragment is exposed alongside the rest of the Actuator
health endpoints:

```
GET /actuator/health/outbox
```

Example body when the engine is running normally:

```json
{
  "status": "UP",
  "details": {
    "state": "RUNNING",
    "totalPending": 42,
    "totalProcessing": 3,
    "totalDisabled": 0,
    "takenAt": "2026-05-01T09:18:27.103Z",
    "workerId": "api-01-4817-a3f2b1c9"
  }
}
```

### Field reference

| Field | Meaning |
|---|---|
| `status` | `UP` iff the engine is `RUNNING` **and** the storage metrics snapshot was readable. Otherwise `DOWN`. |
| `state` | Engine lifecycle state: `STOPPED`, `RUNNING`, or `STOPPING`. |
| `totalPending` | Events in `PENDING` across every type. |
| `totalProcessing` | Events currently claimed by any worker (not just this one). |
| `totalDisabled` | Terminal-failure rows (exhausted retries or explicit `Fail`). |
| `takenAt` | Wall-clock time the metrics snapshot was read. A stale value (older than twice `metrics-cache-ttl`) means no worker has queried the store for a while. |
| `workerId` | Identifier of this JVM's worker row in `event_outboxer.workers`. |
| `metricsError` | Present **only** when the metrics snapshot threw; typically a DB connectivity problem. Also flips `status` to `DOWN`. |

### When it goes DOWN

- **Engine stopped or stopping**: `state != RUNNING`. Happens during
  shutdown and briefly during startup; sustained `DOWN` with
  `state=STOPPED` means the `SmartLifecycle` start failed — check
  logs for `failed to start engine`.
- **Storage unreachable**: a `metricsError` details key is present.
  Same root cause as `/actuator/health/db` going red — check the
  JDBC pool, not the outbox.

### Two recipes

**Dashboard healthy indicator.** Probe `/actuator/health/outbox` every
30 s; green when `status == UP`. Include `workerId` and `totalPending`
on the tile so operators can distinguish "quiet outbox" from "outbox
missing".

**Alerting on DOWN.** Fire an alert when `status=DOWN` for two
consecutive probes (avoid flapping on planned restarts). If the alert
carries `metricsError`, escalate to the DBA; if it carries
`state=STOPPED`, escalate to the service owner.

---

## Kubernetes probes

Two ways to surface the outbox's state to your orchestrator. Pick one
per deployment — **they are not mutually exclusive**, but using both
means k8s restarts pods for the same condition that also fires a
metric alert.

### Option A — let probes drain and kill pods

Services whose k8s manifest probes only `/actuator/health/liveness`
and `/actuator/health/readiness` can opt in to having the outbox
indicator contribute to those groups. Add to `application.yml`:

```yaml
outbox:
  health:
    probe-groups:
      - readiness   # drains traffic during rolling restarts and on engine crash
      - liveness    # triggers pod restart on engine crash
```

What this does: an `EnvironmentPostProcessor` in the starter merges
`outbox` into `management.endpoint.health.group.<name>.include` for
each listed group, preserving the default
`<name>State` contributor and any includes you set yourself. After
this:

| Lifecycle phase | `/actuator/health/readiness` | k8s behaviour |
|---|---|---|
| App starting, engine `STOPPED` | `DOWN` | pod not yet in service rotation |
| Engine `RUNNING` | `UP` | traffic flows |
| Poller thread dies (post-start crash) | `DOWN` within one `watchdog-interval` | pod drained; with liveness in the list, pod is restarted |
| SIGTERM received, engine `STOPPING`/`STOPPED` | `DOWN` | pod taken out of rotation; in-flight handlers drain up to `outbox.maintenance.shutdown-timeout` |

Crash detection is driven by the maintenance scheduler's
`EngineHealthCheckTask`: every `watchdog-interval` it inspects each
per-type poller's thread via `Poller.isCrashed()` and, on the first
dead thread, calls `OutboxEngine.markCrashed(...)`. The engine then
reports `state() == STOPPED`, the health indicator flips DOWN, the
`event_outboxer.engine.state` gauge flips to `stopped=1`, and
`OutboxListener.onEngineCrashed(...)` fires. The SmartLifecycle
wrapper's `isRunning()` stays `true` so Spring still calls
`stop()` on context close and the normal cleanup (worker deregister,
handler drain) runs.

Scope limit in `0.1.0`: the detector watches thread aliveness, which
catches uncaught `Error`s (OOM, StackOverflow) that bypass the
`Poller.tick()` exception filter. It does not catch "thread alive
but stuck in a deadlock" — the handler-level watchdog covers that
scenario for individual handlers. Crash detection also dies with the
maintenance scheduler if something externally shuts that down;
addressing both is a post-MVP refinement.

### Option B — keep probes out of it, alert on a metric

Services that prefer human intervention over automated pod restarts
leave the probe groups unchanged and alert on the `engine.state`
gauge instead:

```
event_outboxer.engine.state{state="stopped"}  0
event_outboxer.engine.state{state="running"}  1
event_outboxer.engine.state{state="stopping"} 0
```

Prometheus rule:

```yaml
- alert: OutboxEngineDown
  expr: event_outboxer_engine_state{state="running"} == 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "outbox engine not RUNNING on {{ $labels.instance }}"
    description: "pod is up but the outbox engine state is != RUNNING"
```

The `for: 1m` window filters out the short interval between MeterRegistry
initialisation and `SmartLifecycle.start()`, as well as the planned
`STOPPING` phase during rolling restarts. Tune longer if you tolerate
longer unresponsive periods.

---

## Micrometer metrics reference

The starter registers `MicrometerOutboxListener` when a
`MeterRegistry` bean exists. Metric names carry the prefix
`event_outboxer` by default — a specific name chosen so the library
does not clash with other sources of `outbox.*` metrics. Override via
`outbox.metrics.prefix` (Spring Boot starter) or the second argument
to `new MicrometerOutboxListener(registry, prefix)` (plain Java).

Every per-event metric carries an `event_type` tag so dashboards can
drill down. Types without a tag are engine-wide and fire once per
worker JVM.

| Metric | Type | Tags | When emitted | Interpretation |
|---|---|---|---|---|
| `event_outboxer.events.published` | counter | `event_type` | after `OutboxEventPublisher.publish(...)` persists a `PendingEvent` | publish throughput per type |
| `event_outboxer.events.claimed` | counter | `event_type` | after `EventStore.claim(...)` returns a batch and the dispatcher picks up an event | claim throughput; low value with growing `pending` → poller starvation |
| `event_outboxer.events.processed` | counter | `event_type` | handler returned `Success`, storage acknowledged the finalize | success rate per type |
| `event_outboxer.events.processing_time` | timer | `event_type` | around `handler.handle(...)`, recorded on successful finalize | tail latency per type — use `p99` for SLOs |
| `event_outboxer.events.attempts` | summary | `event_type` | on successful finalize, records the attempt count that eventually succeeded | values > 1 mean transient failures recovered — large mean = flaky handler |
| `event_outboxer.events.retry_scheduled` | counter | `event_type` | failure chain decided `RetryAt`, storage acknowledged `markForRetry` | retry rate — healthy at small values, watch the ratio `retry_scheduled / processed` |
| `event_outboxer.events.disabled` | counter | `event_type` | failure chain decided `Disable`, storage acknowledged `markDisabled` | exhaustion rate — **any sustained non-zero value deserves triage** |
| `event_outboxer.events.deleted` | counter | `event_type` | failure chain decided `Delete` (rare) | only fires if a custom `FailureHandler` uses `Delete` — usually zero |
| `event_outboxer.events.skipped` | counter | `event_type` | handler returned `Skip` | idempotent no-ops (handler saw the event was already processed) |
| `event_outboxer.events.unknown_type` | counter | `event_type` | claimed an event with no registered handler (see `UnknownHandlerPolicy`) | spikes after a deploy where two instances disagree on the handler set |
| `event_outboxer.events.serialization_errors` | counter | `event_type` | payload could not be deserialised into `handler.payloadType()` | DTO schema drift; the engine disables the event |
| `event_outboxer.handler.errors` | counter | `event_type` | handler threw an uncaught exception | spike → application bug or downstream outage |
| `event_outboxer.handler.stuck_reclaimed` | counter | `event_type` | watchdog force-reclaimed a handler exceeding `handlerMaxRuntime` | non-zero = your handler is slower than you expected |
| `event_outboxer.lock.acquisition_failed` | counter | `event_type` | `EntityLocker.tryLock(...)` returned empty or threw | busy-lock path — safe up to a point; rising value means contention or locker backend trouble |
| `event_outboxer.lock.release_failed` | counter | `event_type` | `LockHandle.close()` threw | Redis/PG returning errors on release — check the locker's backend |
| `event_outboxer.workers.registered` | counter | — | once per `OutboxEngine.start()` | increases by 1 per app restart (and per replica) |
| `event_outboxer.workers.graceful_stops` | counter | — | once per graceful shutdown, after `workers.graceful_stop = TRUE` | equal to `workers.registered` over long windows means no crashes |
| `event_outboxer.workers.deregistered` | counter | — | after `DELETE FROM event_outboxer.workers` on shutdown | same semantics as `graceful_stops` |
| `event_outboxer.heartbeat.failed` | counter | — | `WorkerRegistry.heartbeat(...)` threw or returned `false` | DB connectivity hiccup — sustained non-zero triggers orphan recovery from peers |
| `event_outboxer.orphans.reclaimed` | counter | — | `OrphanRecoveryTask` reclaimed at least one event; value is the number of events moved back to `PENDING` | positive means a peer crashed and this instance took over |
| `event_outboxer.orphans.dead_workers` | counter | — | same trigger as above; counts the number of distinct dead workers | |
| `event_outboxer.lease_renewal_mismatch` | counter | — | lease renewal affected fewer rows than expected (reserved; not used in MVP) | always zero in `0.1.0` |
| `event_outboxer.storage.errors` | counter | `operation` | any storage call raised a `StorageException` | `operation` tag values: `claim[TYPE]`, `save`, `findDead`, etc. |
| `event_outboxer.dispatch.rejected` | counter | `event_type` | per-type handler executor rejected the dispatch | pool + queue saturated — the event is rescheduled shortly |
| `event_outboxer.engine.state` | gauge | `state` | always present — one time series per engine state (`stopped`, `running`, `stopping`); value is 1 for the current state, 0 for the others | primary signal for metric-based alerting on engine liveness. See [§Kubernetes probes](#kubernetes-probes) for the alternative probe-based approach. |
| `event_outboxer.engine.crashed` | counter | — | once per detected crash (poller thread death), incremented by `markCrashed(...)` | any non-zero value is an incident — pair with `engine.state{state="running"}==0` to distinguish crash from planned stop. |
| `event_outboxer.events.pending` | gauge | `event_type` | pulled from `EventStore.metricsSnapshot()` at scrape time; one row per registered handler's event type | backlog graph. Aggregate in PromQL: `sum without(event_type)(event_outboxer_events_pending)`. |
| `event_outboxer.events.processing` | gauge | `event_type` | as above — count of currently-claimed rows | useful alongside `pending` to see how fast handlers drain the queue |
| `event_outboxer.events.disabled` | gauge | `event_type` | as above — count of terminal-failure rows | rising without bound means retries are exhausting permanently; investigate handler errors |
| `event_outboxer.events.oldest_pending_age_seconds` | gauge | `event_type` | seconds since the oldest PENDING row of this type became eligible; `0` when empty | the alertable "am I falling behind?" signal — trigger when age exceeds SLO (e.g. >120 s) |
| `event_outboxer.events.oldest_claimed_age_seconds` | gauge | — | seconds since the oldest PROCESSING row was claimed; `0` when nothing in-flight | pair with `handlerMaxRuntime` — early warning before the watchdog force-reclaims |

### Quick checks on this table

- Every counter / timer / summary above maps 1:1 to a
  `registry.counter(...)` / `registry.timer(...)` / `registry.summary(...)`
  call in `MicrometerOutboxListener`.
- The `engine.state` gauges are published from the Spring Boot starter
  (via `MicrometerAutoConfiguration`) because the listener itself has
  no reference to the engine. They are registered eagerly at context
  refresh so they appear even before `SmartLifecycle.start()` runs —
  with `state="stopped"=1` until the engine is actually started.
- The `event_outboxer.events.*` backlog gauges are also published from
  the starter (`MicrometerAutoConfiguration.outboxBacklogGauges`).
  Every scrape reads `EventStore.metricsSnapshot()` once per gauge,
  which goes through the `MetricsSnapshotCache` SPI — so the database
  is only hit once per cache TTL (default 30 s) regardless of how many
  per-type rows exist. Switch to `outbox.cache.type=redis` to share the
  snapshot across pods so dashboards aggregate to a single value across
  the fleet instead of averaging divergent per-pod caches.

---

## OutboxListener callback catalogue

`OutboxListener` is the engine's observability event bus. Register any
number of beans implementing it; `OutboxListenerRegistry` fans out to
all of them with try/catch isolation, so a broken listener cannot take
another down. Every method has a no-op default — implement only what
you care about.

| # | Callback | Stage | Fires when | Key fields on the `*Info` record |
|---|---|---|---|---|
| 1 | `onEventPublished` | publish | after `EventStore.save(...)` returns, **before the caller's transaction commits** (so a rollback means the listener fired but no event was persisted) | `eventId`, `eventType`, `runAt`, `priority` |
| 2 | `onEventClaimed` | dispatch | after a claim-batch row is picked up by the dispatcher | `eventId`, `eventType`, `attempts`, `claimedAt`, `workerId` |
| 3 | `onEventProcessed` | dispatch | handler returned `Success`, finalize acknowledged | `eventId`, `eventType`, `attempts`, `duration` |
| 4 | `onEventSkipped` | dispatch | handler returned `Skip` (idempotent no-op) | `eventId`, `eventType`, `reason` |
| 5 | `onEventRetryScheduled` | dispatch | failure chain decided `RetryAt`, finalize acknowledged | `eventId`, `eventType`, `attempts`, `nextRunAt`, `reason`, `cause` |
| 6 | `onEventDisabled` | dispatch | failure chain decided `Disable`, finalize acknowledged | `eventId`, `eventType`, `attempts`, `reason`, `cause` |
| 7 | `onEventDeleted` | dispatch | failure chain decided `Delete` (custom handlers only) | `eventId`, `eventType`, `attempts`, `reason` |
| 8 | `onHandlerError` | errors | handler threw an uncaught exception — **fires before** `onEventRetryScheduled` / `onEventDisabled` | `eventId`, `eventType`, `attempts`, `cause` |
| 9 | `onUnknownEventType` | errors | claim returned an event with no registered handler | `eventId`, `eventType` |
| 10 | `onEventSerializationError` | errors | payload could not be deserialised into `handler.payloadType()` | `eventId`, `eventType`, `payloadClass`, `cause` |
| 11 | `onLockAcquisitionFailed` | errors | `EntityLocker.tryLock(...)` returned empty or threw — **informational**, not an error | `eventId`, `eventType`, `lockKey` |
| 12 | `onLockReleaseFailed` | errors | `LockHandle.close()` threw (locker backend refused release) | `eventId`, `eventType`, `lockKey`, `cause` |
| 13 | `onWorkerRegistered` | worker | once per engine start, after the `event_outboxer.workers` row is inserted | `info` (full `WorkerInfo`) |
| 14 | `onWorkerGracefulStop` | worker | once per graceful shutdown, after `graceful_stop = TRUE` | `workerId` |
| 15 | `onWorkerDeregistered` | worker | once per graceful shutdown, after `DELETE FROM event_outboxer.workers` | `workerId` |
| 16 | `onHeartbeatFailed` | worker | periodic heartbeat write threw or affected zero rows | `workerId`, `cause` |
| 17 | `onOrphansReclaimed` | recovery | `OrphanRecoveryTask` moved ≥1 row back to `PENDING` | `deadWorkers` collection, `eventCount` |
| 18 | `onStuckHandlerReclaimed` | recovery | watchdog force-reclaimed a handler exceeding `handlerMaxRuntime` | `eventId`, `eventType`, `elapsed`, `workerId` |
| 19 | `onStorageError` | storage | any storage call raised a `StorageException` | `operation`, `cause` |
| 20 | `onDispatchRejected` | dispatch | per-type handler executor rejected via `RejectedExecutionException` | `eventId`, `eventType`, `cause` |
| 21 | `onEngineCrashed` | engine | the background health check detected that a critical component (typically a poller thread) is no longer alive | `reason`, `cause` (nullable — uncaught `Error` that killed the thread is usually lost), `at`, `workerId` |

### Writing custom listeners

Listeners run on the engine's hot path — worker threads, poller threads
and the shared maintenance executor. Keep them fast and non-blocking;
offload anything substantial to a dedicated executor owned by the
listener. Every invocation is wrapped in try/catch by the registry, so
uncaught exceptions are logged and swallowed.

---

## Troubleshooting playbook

Five scenarios that account for the majority of support questions, each
with the metric or log line that confirms the hypothesis and the usual
fix.

### 1. Backlog is growing unbounded

**Symptom**: `totalPending` in the health indicator climbs and never
falls; `event_outboxer.events.claimed` is flat.

**Diagnose**:
- Check `event_outboxer.events.published` rate vs `event_outboxer.events.processed`
  rate per `event_type`. If `published > processed`, handlers are the
  bottleneck.
- Are the pollers running? Check the engine logs for `poller start:
  eventType=…` once per type at startup.
- Is the handler pool saturated? `event_outboxer.dispatch.rejected` non-zero
  confirms it — raise `event_outboxer.event-types.defaults.handler-pool-size`
  or `handler-queue-capacity`.

**Fix**: scale the handler pool for the affected type, or add another
replica. Per-type isolation means fixing one type does not help
another — tune each hot type separately.

### 2. Events accumulating in `DISABLED`

**Symptom**: `event_outboxer.events.disabled` rising steadily; `totalDisabled`
never drops.

**Diagnose**:
- Find the rows: `SELECT id, event_type, attempts, last_fail_reason
  FROM event_outboxer.events WHERE status = 'DISABLED' ORDER BY created_at
  DESC LIMIT 50;`.
- Correlate with `event_outboxer.handler.errors` and application logs — look
  for a single recurring exception across all the disabled events.
- If `event_outboxer.events.serialization_errors` is the culprit, a DTO shape
  change is incompatible with events persisted by a previous version.

**Fix**: fix the handler, then re-enable the affected rows:

```sql
UPDATE event_outboxer.events
SET status = 'PENDING', attempts = 0, last_fail_reason = 'manually re-enabled',
    version = version + 1
WHERE id = ANY(...) AND status = 'DISABLED';
```

### 3. Orphan recovery doesn't trigger after a crash

**Symptom**: a replica crashed and its events are stuck in `PROCESSING`
for longer than `dead-threshold`.

**Diagnose**:
- `SELECT worker_id, last_heartbeat, graceful_stop FROM event_outboxer.workers
  WHERE last_heartbeat < now() - interval '90 seconds';`. If the dead
  row still has `graceful_stop = TRUE` it will **never** be reaped —
  that's intentional; the crash happened during shutdown.
- Check that the peer's `OrphanRecoveryTask` is running: look for the
  `orphans reclaimed …` log line in the surviving replica.

**Fix**: for `graceful_stop = TRUE` leftovers, delete the worker row
manually and call `reclaimOrphans` via a one-off admin script, OR just
wait — the next startup of that replica will overwrite the row and
resume.

### 4. "unknown event type" warnings after a deploy

**Symptom**: `event_outboxer.events.unknown_type` spiking shortly after a
rollout; logs show `unknown event type …`.

**Diagnose**:
- Two replicas disagree on the handler set — one is on old code that
  published an event type the new code no longer handles, or vice
  versa. Compare the handler bean lists across replicas.
- With `UnknownHandlerPolicy=SKIP` (default) the events are
  rescheduled with a delay, so they recover once every replica is on
  the new version.

**Fix**: either wait for the rollout to complete, or set the policy to
`DISABLE` if the type is truly gone and you want to fail loud.

### 5. Lock-busy retries climbing

**Symptom**: `event_outboxer.lock.acquisition_failed` growing quickly;
`event_outboxer.events.retry_scheduled` has a big share labelled `lock busy`.

**Diagnose**:
- High legitimate contention: many events target the same aggregate
  (same `extractLockKey(payload)` value) and queue up.
- Stuck holder: a handler that forgot to return and still holds a
  Redis lock. Look for `event_outboxer.handler.stuck_reclaimed` — if zero, the
  holder is alive but slow.
- Locker backend issue: `event_outboxer.lock.release_failed` non-zero hints
  at Redis / PG connectivity trouble.

**Fix**: if the bottleneck is an aggregate hot-spot, partition the
lock key more finely (include a sub-aggregate); if the locker
backend is flaky, switch the adapter or add backoff. Redis TTL will
eventually release a stuck lock even if `LockHandle.close()` never
runs.

### 6. Engine crashed (poller thread died)

**Symptom**: `event_outboxer.engine.crashed` incremented by 1;
`event_outboxer.engine.state{state="running"}` flipped to 0, `state="stopped"=1`;
`/actuator/health/outbox` is DOWN; log contains
`ENGINE CRASHED on worker …`.

**Diagnose**:
- A poller thread exited unexpectedly. Check the pod's stderr /
  captured stdout for an uncaught `Error` stack trace — the JVM's
  default uncaught-exception handler writes one when a thread dies.
  Typical culprits: `OutOfMemoryError` from a handler or strategy
  allocation, `StackOverflowError` from unbounded recursion, or a
  native crash wrapped as `UnsatisfiedLinkError`.
- `OutboxListener.onEngineCrashed(...)` carries a `reason` pointing
  at the affected `eventType` but usually loses the `cause` (the
  thread dies before reporting). Correlate with the stderr stack
  trace by timestamp.

**Fix**:
- If liveness is in `outbox.health.probe-groups`, k8s restarts the
  pod automatically on the next probe cycle.
- Without liveness integration, restart the pod manually or let the
  Prometheus alert rule (`engine_state{state="running"}==0 for 1m`)
  escalate.
- Post-mortem: fix the underlying cause — heap sizing, stricter
  validation on the handler's input, etc. The engine does not
  attempt to restart itself in-process.

---

## Related documents

- [docs/ARCHITECTURE.md](ARCHITECTURE.md) — engine design and data flows.
- [docs/STORAGE.md](STORAGE.md) — SQL schema and operational queries.
- [docs/CONFIGURATION.md](CONFIGURATION.md) — every tunable property.
- [docs/TESTING.md](TESTING.md) — asserting over the same signals in
  your own tests via `event-outboxer-testkit`.
