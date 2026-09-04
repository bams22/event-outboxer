# Observability

How to tell what event-outboxer is doing from the outside: the
Actuator health indicator, the Micrometer metrics catalogue, the
`OutboxListener` callback reference, distributed tracing, and a short
troubleshooting playbook for the seven most common production
scenarios.

## Contents

1. [Health indicator](#health-indicator)
2. [Kubernetes probes](#kubernetes-probes)
3. [Micrometer metrics reference](#micrometer-metrics-reference)
4. [OutboxListener callback catalogue](#outboxlistener-callback-catalogue)
5. [Distributed tracing](#distributed-tracing)
6. [Troubleshooting playbook](#troubleshooting-playbook)

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
| SIGTERM received, engine `STOPPING`/`STOPPED` | `DOWN` | pod taken out of rotation; in-flight handlers drain up to `event-outboxer.maintenance.shutdown-timeout` |

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

A ready-to-import Grafana dashboard covering every metric below —
health, throughput, latency, backlog, errors, saturation, locks and
maintenance, with `environment` / `service` / `pod` / `eventType`
template variables — lives in
[docs/grafana/](grafana/README.md).

The starter registers `MicrometerOutboxListener` when a
`MeterRegistry` bean exists. Metric names carry the prefix
`event_outboxer` by default — a specific name chosen so the library
does not clash with other sources of `outbox.*` metrics. Override via
`event-outboxer.metrics.prefix` (Spring Boot starter) or the second argument
to `new MicrometerOutboxListener(registry, prefix)` (plain Java).

Every per-event metric in the catalogue below carries an `event_type`
tag so dashboards can drill down. Types without a tag are engine-wide
and fire once per worker JVM. (The side-effect meters the tracing
adapter contributes are the one exception — they key the event type as
`messaging.destination.name`; see
[§Side-effect meters from the tracing adapter](#side-effect-meters-from-the-tracing-adapter).)

In a Spring Boot app three of the timers below publish SLO histogram
buckets out of the box: the starter automatically applies the defaults
shipped with the metrics module
(`META-INF/event-outboxer/metrics-defaults.yml` —
a 10ms–1h grid for `queue_time`, 10ms–10m for `processing_time` (covers the default 5m handler-max-runtime budget), 30s–1h for
`stuck_time`; ~19 extra series per tag combination). The two
diagnostic timers (`poller.claim_time`, `handler.error_time`)
deliberately ship **no** buckets — they publish count/sum/max only,
which keeps the series budget down; averages and rates need nothing
more, and you can add `management.metrics.distribution.*` boundaries
yourself if you want quantiles. The `_bucket`
series aggregate across pods, so fleet-wide `histogram_quantile()`
works with precision limited to the grid. Your own
`management.metrics.distribution.*` settings always override the
defaults; disable them entirely with
`event-outboxer.metrics.distribution-defaults.enabled: false`. For
finer quantiles add
`management.metrics.distribution.percentiles-histogram` (~70 series)
on top — boundaries merge.

| Metric | Type | Tags | When emitted | Interpretation |
|---|---|---|---|---|
| `event_outboxer.events.published` | counter | `event_type` | after `OutboxEventPublisher.publish(...)` persists a `PendingEvent` | publish throughput per type |
| `event_outboxer.events.coalesced` | counter | `event_type` | a keyed publish coalesced into an existing PENDING event instead of inserting (ADR-0021); fires instead of `events.published` for that request | the dedup ratio: `coalesced / (published + coalesced)` says whether your dedup keys actually collapse anything — a flat zero with keys configured means the keys never repeat |
| `event_outboxer.events.queue_time` | timer | `event_type` | after `EventStore.claim(...)` returns a batch and the dispatcher picks up an event; records `claimedAt - createdAt` (clamped at 0 on clock skew) | how long events wait in the outbox before a worker picks them up — the end-to-end lag signal; a rising `p99` means pollers or handler pools cannot keep up. Its `_count` series is the claim throughput (the removed `events.claimed` counter) |
| `event_outboxer.events.processing_time` | timer | `event_type` | around `handler.handle(...)`, recorded on successful finalize | tail latency per type — use `p99` for SLOs. Its `_count` series is the success rate (the removed `events.processed` counter) |
| `event_outboxer.events.attempts` | summary | `event_type`, `outcome` | on processed, disabled and deleted finalizes, records the final attempt count; `outcome` is `processed`, `disabled` or `deleted` | values > 1 mean transient failures recovered — large mean = flaky handler; the `outcome` tag separates the healthy population from the budget doomed events burned before dying |
| `event_outboxer.events.retry_scheduled` | counter | `event_type`, `reason` | a retry was scheduled; `reason` is a bounded set: `failure_decision`, `lock_busy`, `unknown_handler`, `dispatch_rejected` | retry rate — healthy at small values, watch the ratio `retry_scheduled / processed`; the `reason` tag separates handler failures from backpressure and lock contention |
| `event_outboxer.events.disabled` | counter | `event_type`, `reason` | the event was disabled; `reason` is a bounded set: `failure_decision`, `failure_handler_error`, `unknown_handler` | exhaustion rate — **any sustained non-zero value deserves triage** |
| `event_outboxer.events.deleted` | counter | `event_type` | failure chain decided `Delete` (rare) | only fires if a custom `FailureHandler` uses `Delete` — usually zero |
| `event_outboxer.events.skipped` | counter | `event_type` | handler returned `Skip` | idempotent no-ops (handler saw the event was already processed). Deliberately **no** `reason` tag: the skip reason is user-supplied free-form text — unbounded tag cardinality |
| `event_outboxer.events.unknown_type` | counter | `event_type` | claimed an event with no registered handler (see `UnknownHandlerPolicy`) | spikes after a deploy where two instances disagree on the handler set |
| `event_outboxer.events.serialization_errors` | counter | `event_type` | payload could not be deserialised into `handler.payloadType()` | DTO schema drift; the engine disables the event |
| `event_outboxer.handler.errors` | counter | `event_type`, `exception` | handler threw an uncaught exception; `exception` is the simple class name of the thrown type | spike → application bug or downstream outage; the `exception` tag separates timeouts from logic bugs |
| `event_outboxer.handler.error_time` | timer | `event_type` | same trigger as `handler.errors`; records how long the failed `handler.handle(...)` attempt ran | separates instant failures (logic bug, NPE) from slow ones (downstream timeouts): a mean near your client timeout means the handler burns its budget waiting. No SLO buckets by default |
| `event_outboxer.handler.stuck_time` | timer | `event_type` | watchdog force-reclaimed a handler exceeding `handlerMaxRuntime`; records how long the stuck handler had been running when reclaimed | how far past `handlerMaxRuntime` handlers actually run — use to right-size the budget. Its `_count` series is the reclaim rate (the removed `handler.stuck_reclaimed` counter) |
| `event_outboxer.handler.abandoned` | counter | `event_type` | a force-reclaimed dispatch was still running `abandonedHandlerGrace` later — it ignored the interrupt, or the type set `interrupt-stuck-handler: false` and was never asked to stop | **every increment is a handler thread this JVM cannot get back until the handler returns on its own** — normally blocked in something without a timeout; the row itself is safe (already back in PENDING). `HandlerAbandonedInfo.interrupted` and the log level (ERROR vs WARN) separate the runaway handler from the configured opt-out |
| `event_outboxer.lock.acquisition_failed` | counter | `event_type`, `outcome` | `EntityLocker.tryLock(...)` returned empty (`outcome="busy"`) or threw (`outcome="error"`) | `busy` is normal contention, safe up to a point; `error` means the locker backend is failing — alert on it separately |
| `event_outboxer.lock.wait_time` | timer | `event_type`, `outcome` | every `EntityLocker.tryLock(...)` for a handler that declares a lock key: `outcome="acquired"` records how long the acquisition took (≈0 when the key was free, up to the type's `lock-wait` when it was busy first); `outcome="busy"` records the whole spent `lock-wait` budget when the engine gave up (ADR-0035) | the `acquired` series' `_count` is the total number of acquisitions and its histogram shows which share needed the bounded wait — a large share near the `lock-wait` ceiling means the wait is too short for the holders' hold time; a growing `busy` count next to it means the wait is spent for nothing (slow holder, dead holder's lease) and `lock-wait` should come down for that type |
| `event_outboxer.lock.release_failed` | counter | `event_type` | `LockHandle.close()` threw | Redis/PG returning errors on release — check the locker's backend |
| `event_outboxer.workers.registered` | counter | — | once per `OutboxEngine.start()` (and once per heartbeat-driven re-registration after a peer reaped the row) | increases by 1 per app restart (and per replica) |
| `event_outboxer.workers.graceful_stops` | counter | — | once per graceful shutdown, after `workers.graceful_stop = TRUE` | equal to `workers.registered` over long windows means no crashes |
| `event_outboxer.heartbeat.failed` | counter | — | `WorkerRegistry.heartbeat(...)` threw or returned `false` | DB connectivity hiccup — sustained non-zero triggers orphan recovery from peers. Pair with the `heartbeat.last_success_age_seconds` gauge, which also catches the failure mode this counter cannot: a stalled maintenance scheduler |
| `event_outboxer.orphans.reclaimed` | counter | — | `OrphanRecoveryTask` reclaimed at least one event; value is the number of events moved back to `PENDING` | positive means a peer crashed and this instance took over |
| `event_outboxer.storage.errors` | counter | `operation` | a storage call failed; `operation` is a small stable set: `claim[TYPE]` (per-type claim query), `save` (publish-side insert), `finalize` (markProcessed / markForRetry / markDisabled), `release` (recovery release after a failed finalize) | any sustained rate is a DB incident. A finalize failure whose recovery release also fails increments twice (`finalize` + `release`) — two distinct failed operations, mind the double count in alerts |
| `event_outboxer.maintenance.runs` | counter | `task`, `result` | after every run of a periodic maintenance task; `task` is `heartbeat`, `orphan_recovery`, `watchdog`, `engine_health_check`, `retention` or `stale_claim_sweeper`; `result` is `ok` or `failed` | the liveness signal for the background machinery: a sustained `failed` rate for one task deserves triage (previously these failures were WARN log lines only), and a task whose `ok` rate flatlines has silently stopped |
| `event_outboxer.poller.polls` | counter | `event_type`, `result` | after every claim attempt of the per-type poller; `result` is `claimed` or `empty` | poll cadence and hit rate; a high `empty` share with low lag is healthy idling, a high `empty` share with growing backlog means events are scheduled in the future (backoff) |
| `event_outboxer.poller.claim_time` | timer | `event_type` | same trigger as `poller.polls`; records the wall time of the claim query itself, empty polls included | attributes a rising `queue_time` : slow claims (index bloat, lock contention on `FOR UPDATE SKIP LOCKED`, DB overload) look completely different from saturated handler pools. No SLO buckets by default |
| `event_outboxer.poller.saturated` | counter | `event_type` | poller skipped a claim cycle because the handler executor had no free capacity (zero — waiting below a `claim-min-free` threshold above zero is not counted) | each increment ≈ one skipped poll cycle; a sustained rate means the type's pool/queue budget is undersized |
| `event_outboxer.claims.stale_swept` | counter | — | stale-claim sweeper released abandoned PROCESSING rows back to PENDING; incremented by the swept count | any non-zero value indicates a bug or incident — these rows were invisible to the watchdog and orphan recovery |
| `event_outboxer.retention.purged` | counter | `kind` | retention task deleted rows past their window; `kind` is `archive` or `disabled` | confirms retention is actually running; a flat line with retention enabled now pairs with `maintenance.runs{task="retention",result="failed"}` to say *why* |
| `event_outboxer.engine.state` | gauge | `state` | always present — one time series per engine state (`stopped`, `running`, `stopping`); value is 1 for the current state, 0 for the others | primary signal for metric-based alerting on engine liveness. See [§Kubernetes probes](#kubernetes-probes) for the alternative probe-based approach. |
| `event_outboxer.engine.crashed` | counter | — | once per detected crash (poller thread death), incremented by `markCrashed(...)` | any non-zero value is an incident — pair with `engine.state{state="running"}==0` to distinguish crash from planned stop. |
| `event_outboxer.events.backlog` | gauge | `event_type`, `status` | pulled from `EventStore.metricsSnapshot()` at scrape time; one row per registered handler's event type and lifecycle status (`pending`, `processing`, `disabled`) | backlog graph. `status="pending"` — waiting rows; `status="processing"` — currently-claimed rows (shows how fast handlers drain the queue); `status="disabled"` — terminal-failure rows (rising without bound means retries are exhausting permanently). Aggregate in PromQL: `sum without(event_type)(event_outboxer_events_backlog{status="pending"})`. |
| `event_outboxer.events.oldest_pending_age_seconds` | gauge | `event_type` | seconds since the oldest PENDING row of this type became eligible; `0` when empty | the alertable "am I falling behind?" signal — trigger when age exceeds SLO (e.g. >120 s) |
| `event_outboxer.events.oldest_claimed_age_seconds` | gauge | — | seconds since the oldest PROCESSING row was claimed; `0` when nothing in-flight | pair with `handlerMaxRuntime` — early warning before the watchdog force-reclaims |
| `event_outboxer.events.in_flight` | gauge | `event_type` | read off the engine's in-flight registry at scrape time (no storage round trip) | events this JVM is processing right now; sums across replicas to fleet-wide concurrency |
| `event_outboxer.handler.executor.free_capacity` | gauge | `event_type` | remaining submission budget of the type's handler executor; `0` while saturated or engine stopped | sustained `0` with a running engine = the type is saturated — correlate with `poller.saturated` and `retry_scheduled{reason="dispatch_rejected"}` |
| `event_outboxer.handler.executor.capacity` | gauge | `event_type` | constant budget `handlerPoolSize + handlerQueueCapacity` (uniform for platform and virtual executors) | `capacity - free_capacity` = queued + running; useful as the denominator in a utilisation panel |
| `event_outboxer.handler.abandoned_threads` | gauge | `event_type` | force-reclaimed dispatches of this type whose thread has still not returned | the leak counter: it drops back only when a thread finally returns. Approaching `handler.executor.capacity` means the type is about to stop processing entirely — alert well before that |
| `event_outboxer.heartbeat.last_success_age_seconds` | gauge | — | seconds since this worker's last successful heartbeat write; `NaN` until the first one (Prometheus drops NaN samples — alert with `absent()`-aware rules or a plain `>` comparison) | the "is the background machinery alive?" gauge: it keeps growing when the maintenance scheduler is stalled even though `heartbeat.failed` stays flat. Alert when it exceeds ~3× the heartbeat interval |
| `event_outboxer.entity_locks.held` | gauge | — | lease-locker only (`lock.type=postgres-lease`): currently held entity-lock leases (`expires_at > now()`); registered by `PostgresLeaseLockAutoConfiguration` | lock-contention level across the fleet. **Costs a `COUNT(*)` against `entity_locks` on every scrape** — it bypasses `MetricsSnapshotCache` (the lease table is not part of the events snapshot); the table is small by construction (≤ in-flight handlers), so this is cheap, but it is the one meter with a per-scrape query. Reads `NaN` while the query fails |

### Quick checks on this table

- Every counter / timer / summary above maps 1:1 to a
  `registry.counter(...)` / `registry.timer(...)` / `registry.summary(...)`
  call in `MicrometerOutboxListener` — with two deliberate absences: a
  timer's Prometheus `_count` series doubles as the throughput counter
  (`queue_time` → claims, `processing_time` → successes, `stuck_time` →
  reclaims), and `retry_scheduled{reason="dispatch_rejected"}` doubles
  as the rejected-dispatch counter, so no standalone counters exist for
  those.
- The `heartbeat.last_success_age_seconds` gauge comes from the starter
  (`MicrometerAutoConfiguration.outboxHeartbeatGauge`, reading
  `OutboxEngine.lastHeartbeatSuccessAt()`), and `entity_locks.held`
  from `PostgresLeaseLockAutoConfiguration` — neither is a listener
  metric.
- The `engine.state` gauges are published from the Spring Boot starter
  (via `MicrometerAutoConfiguration`) because the listener itself has
  no reference to the engine. They are registered eagerly at context
  refresh so they appear even before `SmartLifecycle.start()` runs —
  with `state="stopped"=1` until the engine is actually started.
- The `event_outboxer.events.backlog` and `oldest_*_age_seconds` gauges
  are also published from
  the starter (`MicrometerAutoConfiguration.outboxBacklogGauges`).
  The backlog gauge is a single meter name with a `status` tag on
  purpose: a per-status gauge named `events.disabled` would collide
  with the `events.disabled` *counter* from the listener (same meter id,
  different type — Micrometer rejects the second registration).
  Every scrape reads `EventStore.metricsSnapshot()` once per gauge,
  which goes through the `MetricsSnapshotCache` SPI — so the database
  is only hit once per cache TTL (default 30 s) regardless of how many
  per-type rows exist. Switch to `event-outboxer.cache.type=redis` to share the
  snapshot across pods so dashboards aggregate to a single value across
  the fleet instead of averaging divergent per-pod caches.

### Side-effect meters from the tracing adapter

Everything above comes from `event-outboxer-metrics-micrometer`. If
[`event-outboxer-tracing-micrometer`](modules/event-outboxer-tracing-micrometer.md)
is *also* wired, four more meters appear — not from the listener but
from Boot's `DefaultMeterObservationHandler`, which turns every
observation into a `Timer` plus (while
`management.observations.long-task-timer.enabled` stays at its default
`true`) a `LongTaskTimer`. They use the same `event-outboxer.metrics.prefix`
as the table above:

| Metric | Type | Tags | When emitted | Interpretation |
|---|---|---|---|---|
| `event_outboxer.publish` | timer | `messaging.system`, `messaging.operation.type`, `messaging.destination.name`, `error` | publish-side observation | see the caveat below — on the batch path this is **not** per-event latency |
| `event_outboxer.publish.active` | long task timer | same, minus `error` | in-flight publish observations | non-zero only during a publish |
| `event_outboxer.process` | timer | same four keys | handle-side observation, around `handler.handle(...)` only | per-attempt handler latency, **failures included** |
| `event_outboxer.process.active` | long task timer | same, minus `error` | in-flight handler attempts | overlaps `events.in_flight`, which is the cheaper gauge |

Three things to know before you build on them:

- **`process` is not a substitute for `events.processing_time`.** The
  observation wraps only `handler.handle(...)` and is recorded on
  *every* attempt including failures and retries;
  `events.processing_time` measures claim → finalize and only on
  success, and is tagged `event_type` rather than
  `messaging.destination.name`. Keep `events.processing_time` as the
  processing-latency SLI. The `error` tag Micrometer appends (`none`
  or the exception's simple name) is what separates the two
  populations here.
- **`publish` is not per-event latency on the batch path.**
  `publishAll(...)` opens one observation per request and closes them
  all after the single batch insert, so one sample covers the whole
  batch and the rest cover almost nothing. Single `publish(...)` calls
  are accurate; ignore the distribution if the application batches.
- **No SLO buckets, no dashboard panels.**
  `META-INF/event-outboxer/metrics-defaults.yml` covers only the three
  listener timers, and so does
  [the shipped dashboard](grafana/README.md) — these four publish
  count/sum/max and no `_bucket` series unless you add boundaries
  yourself.

If you do not want them, drop the meters and leave tracing alone:

```java
@Bean
MeterFilter denyOutboxTracingMeters() {
    return MeterFilter.deny(
            id ->
                    id.getName().startsWith("event_outboxer.publish")
                            || id.getName().startsWith("event_outboxer.process"));
}
```

Do **not** reach for `management.observations.enable.event_outboxer=false`
instead: that switches off the observations themselves, which silently
takes the PRODUCER/CONSUMER spans and the stored `trace_context` with
them.

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
| 2 | `onEventClaimed` | dispatch | after a claim-batch row is picked up by the dispatcher | `eventId`, `eventType`, `attempts`, `createdAt`, `claimedAt`, `workerId` |
| 3 | `onEventProcessed` | dispatch | handler returned `Success`, finalize acknowledged | `eventId`, `eventType`, `attempts`, `duration` |
| 4 | `onEventSkipped` | dispatch | handler returned `Skip` (idempotent no-op) | `eventId`, `eventType`, `reason` |
| 5 | `onEventRetryScheduled` | dispatch | a retry was scheduled (failure decision, lock busy, unknown handler, or dispatch rejected), finalize acknowledged | `eventId`, `eventType`, `attempts`, `nextRunAt`, `trigger` (bounded enum, safe as a tag), `reason` (free-form), `cause` |
| 6 | `onEventDisabled` | dispatch | failure chain decided `Disable` (or the failure handler threw / unknown-handler policy), finalize acknowledged | `eventId`, `eventType`, `attempts`, `trigger` (bounded enum, safe as a tag), `reason` (free-form), `cause` |
| 7 | `onEventDeleted` | dispatch | failure chain decided `Delete` (custom handlers only) | `eventId`, `eventType`, `attempts`, `reason` |
| 8 | `onHandlerError` | errors | handler threw an uncaught exception — **fires before** `onEventRetryScheduled` / `onEventDisabled` | `eventId`, `eventType`, `attempts`, `cause`, `duration` (time spent in the failed attempt) |
| 9 | `onUnknownEventType` | errors | claim returned an event with no registered handler | `eventId`, `eventType` |
| 10 | `onEventSerializationError` | errors | payload could not be deserialised into `handler.payloadType()` | `eventId`, `eventType`, `payloadClass`, `cause` |
| 11 | `onLockAcquired` | dispatch | `EntityLocker.tryLock(...)` yielded the lock for a handler that declares a lock key, right before the handler runs — every acquisition, immediate ones included | `eventId`, `eventType`, `lockKey`, `waited` (≈0 when the key was free, up to `lock-wait` when it was busy first, ADR-0035) |
| 12 | `onLockAcquisitionFailed` | errors | `EntityLocker.tryLock(...)` returned empty after the type's bounded `lock-wait`, if any (`outcome=BUSY` — normal contention, informational) or threw (`outcome=ERROR` — locker backend failure) | `eventId`, `eventType`, `lockKey`, `outcome`, `waited`, `cause` (null for BUSY) |
| 13 | `onLockReleaseFailed` | errors | `LockHandle.close()` threw (locker backend refused release) | `eventId`, `eventType`, `lockKey`, `cause` |
| 14 | `onWorkerRegistered` | worker | once per engine start, after the `event_outboxer.workers` row is inserted | `info` (full `WorkerInfo`) |
| 15 | `onWorkerGracefulStop` | worker | once per graceful shutdown, after `graceful_stop = TRUE` | `workerId` |
| 16 | `onWorkerDeregistered` | worker | once per graceful shutdown, after `DELETE FROM event_outboxer.workers` | `workerId` |
| 17 | `onHeartbeatFailed` | worker | periodic heartbeat write threw or affected zero rows | `workerId`, `cause` |
| 18 | `onOrphansReclaimed` | recovery | `OrphanRecoveryTask` moved ≥1 row back to `PENDING` | `deadWorkers` collection, `eventCount` |
| 19 | `onStuckHandlerReclaimed` | recovery | watchdog force-reclaimed a handler exceeding `handlerMaxRuntime` | `eventId`, `eventType`, `elapsed`, `workerId`, `interrupted` (whether the handler thread was interrupted) |
| 20 | `onStorageError` | storage | any storage call raised a `StorageException` | `operation`, `cause` |
| 21 | `onDispatchRejected` | dispatch | per-type handler executor rejected via `RejectedExecutionException` | `eventId`, `eventType`, `cause` |
| 22 | `onEngineCrashed` | engine | the background health check detected that a critical component (typically a poller thread) is no longer alive | `reason`, `cause` (nullable — uncaught `Error` that killed the thread is usually lost), `at`, `workerId` |
| 23 | `onPollCompleted` | polling | after every claim attempt of a per-type poller, including empty polls — the **highest-frequency callback** (up to once per `pollMinInterval` per type); keep implementations O(1) | `eventType`, `requested`, `claimed`, `duration` (wall time of the claim query) |
| 24 | `onPollerSaturated` | polling | poller skipped a claim cycle because the handler executor had no free capacity (not fired while merely waiting for the `claim-min-free` refill threshold) | `eventType` |
| 25 | `onStaleClaimsSwept` | maintenance | stale-claim sweeper released abandoned PROCESSING rows back to PENDING (fires only when ≥1 was swept) | `count`, `threshold` |
| 26 | `onRetentionPurged` | maintenance | retention task deleted rows past their window (fires only when ≥1 was purged) | `archivedPurged`, `disabledPurged` |
| 27 | `onHandlerAbandoned` | recovery | fires **once** per force-reclaimed dispatch that was still running `abandonedHandlerGrace` later — its thread keeps a slot of the type's pool until it returns. `interrupted=true`: the handler ignored the interrupt (logged ERROR); `interrupted=false`: the type set `interrupt-stuck-handler: false`, so it was never asked to stop (logged WARN) | `eventId`, `eventType`, `workerId`, `threadName`, `elapsed`, `interrupted` |
| 28 | `onEventCoalesced` | publish | a keyed publish coalesced into an existing PENDING event instead of inserting (ADR-0021) — fires instead of `onEventPublished`, still **before the caller's transaction commits** | `existingEventId`, `eventType`, `dedupKey` (free-form — never a metric tag) |
| 29 | `onMaintenanceRunCompleted` | maintenance | after every run of a periodic maintenance task, OK or FAILED — fired by the scheduler's guarded wrapper, which keeps a throwing task on its schedule | `task` (stable name, safe as a tag), `result`, `cause` (null on OK) |

### Writing custom listeners

Listeners run on the engine's hot path — worker threads, poller threads
and the shared maintenance executor. Keep them fast and non-blocking;
offload anything substantial to a dedicated executor owned by the
listener. Every invocation is wrapped in try/catch by the registry, so
uncaught exceptions are logged and swallowed.

---

## Distributed tracing

The trace active at `publish()` continues into handler execution
(ADR-0023). The mechanics:

1. `publish()` starts a PRODUCER span `outbox publish <eventType>` as
   a child of the caller's current span and captures its context
   (`traceparent` / `tracestate` / `baggage`, or whatever the
   configured propagator emits) into the event row's `trace_context`
   column. An explicit `PublishOptions.traceContext` overrides the
   captured map.
2. When a worker claims the event, the dispatcher starts a CONSUMER
   span `outbox process <eventType>` as a child of the stored context
   and makes it — baggage included — current around
   `handler.handle(...)`. The handler's own spans (HTTP clients, JDBC,
   ...) nest under it automatically. Each retry attempt gets a fresh
   span in the same trace; a handler exception is recorded on the span
   (exception event + ERROR status).
3. **Deferred events link instead of parenting** (ADR-0023, 2026-08-28
   amendment). An event published with a `runAt` further ahead than
   `event-outboxer.tracing.link-threshold` (default 1 minute) would
   otherwise stretch one trace across the whole delay — a two-day
   trace with a hole in it that time-range search never finds and
   tail-based samplers have long since forgotten. So for such events
   the CONSUMER span is a **new root carrying a span link** to the
   PRODUCER span; both spans are tagged
   `event_outboxer.propagation=link`, baggage is still restored, and
   every retry attempt of that event is a linked root too. The
   decision is made once at publish time from the publisher's intent
   (backlog and retry backoff never change a trace's shape) and rides
   in the row's `trace_context` as the extra key
   `event_outboxer.propagation=link`, which the engine strips before
   the carrier reaches an adapter or `EventContext`.
   `event-outboxer.tracing.deferred-propagation: child` restores
   unconditional parent-child continuity. In Grafana Tempo / Jaeger
   the linked trace shows up through the link on the consumer span
   and, in both directions, through `messaging.message.id`.

### Span attributes

| Attribute | Side | Value |
|---|---|---|
| `messaging.system` | both | `event_outboxer` |
| `messaging.operation.type` | both | `send` (publish) / `process` (handle) |
| `messaging.destination.name` | both | event type |
| `messaging.message.id` | both | event UUID |
| `event_outboxer.attempt` | consumer | 1-based attempt number |
| `event_outboxer.worker.id` | consumer | worker executing the handler |
| `event_outboxer.coalesced_into` | producer | id of the existing PENDING event this publish coalesced into (ADR-0021); the surviving row keeps the first publish's context |
| `event_outboxer.propagation` | both | `link` on the spans of a deferred event — the consumer is a new root with a span link to the producer instead of its child; absent otherwise |

### Choosing an adapter

Tracing engages when one of the two adapter modules is on the
classpath (both are `<optional>` in the starter):

| Your setup | Add | Auto-configured when |
|---|---|---|
| Spring Boot Actuator + `micrometer-tracing-bridge-otel` (or `-brave`) | `event-outboxer-tracing-micrometer` | Boot provides `ObservationRegistry` + `Tracer` + `Propagator` beans; the stored carrier follows `management.tracing.propagation.*` and `management.tracing.baggage.remote-fields` |
| OpenTelemetry Java agent, or a hand-built OTel SDK | `event-outboxer-tracing-otel` | always (uses the `OpenTelemetry` bean if present, else `GlobalOpenTelemetry`); an unconfigured instance stores an empty context |
| Both modules present | — | Micrometer wins; the OTel adapter backs off |
| Neither | — | `OutboxTracer.NOOP` — zero cost, empty `trace_context` |

`event-outboxer.tracing.enabled=false` disables both
auto-configurations; a user-defined `OutboxTracer` bean always takes
precedence. Without Spring, wire an adapter directly:
`builder.tracer(new OtelOutboxTracer(openTelemetry))`.

The Micrometer adapter instruments through the Observation API
(ADR-0023, 2026-08-16 amendment), which has three visible effects. The
current *observation* is set around handler invocation, so handler
code that offloads work — `@Async`, `ContextPropagatingTaskDecorator`,
Reactor `contextCapture()` — keeps the trace instead of starting a
detached one. The same observations also register four meters as a
side effect — see
[§Side-effect meters from the tracing adapter](#side-effect-meters-from-the-tracing-adapter)
for what they do and do not measure, and for the `MeterFilter` that
removes them. And, because Boot filters observations by name, they can
be switched off from the outside:
`management.observations.enable.all=false` (or
`...enable.event_outboxer=false`) leaves the adapter wired but silent —
no spans, empty `trace_context`, no error anywhere. Suppress the meters
with a `MeterFilter`, never with that property. The OTel adapter has
neither behaviour: it emits spans only.

Because those observations become meters, their names honour
`event-outboxer.metrics.prefix` just like the listener's — set the
prefix and both modules move namespace together. Note that the event
type lands on them as a real Micrometer tag
(`messaging.destination.name`), which makes this adapter share the
metrics module's assumption that event types are a bounded,
code-defined set — never a per-tenant or per-entity string.

For deferred events the Micrometer adapter needs its own receiver
handler — `OutboxReceiverTracingObservationHandler`, which the starter
registers ahead of Boot's generic one (bean order 900 vs Boot's 1000).
Without it Boot's handler claims the context and the consumer span
silently stays a child; the `event_outboxer.propagation=link` tag on
such a span is the tell-tale. The link target is read from the
carrier in the formats Boot can emit (`traceparent`, `b3`, `X-B3-*`);
a custom propagator yields an unlinked root. The Brave bridge cannot
detach a parent (`setNoParent()` is a no-op there) and renders links
as tags, so on Brave deferred events keep the parent-child shape.

Adapter failures never affect delivery: the engine wraps the tracer
defensively (`SafeOutboxTracer`), so a throwing tracing backend
degrades to no-op with a debug log line.

Deliberately untraced (ADR-0009: background operations must not
pollute traces): poller iterations, heartbeat, orphan recovery,
watchdog, retention, and the finalize write (which may group-commit on
a different thread after the consumer span closed).

---

## Troubleshooting playbook

Five scenarios that account for the majority of support questions, each
with the metric or log line that confirms the hypothesis and the usual
fix.

### 1. Backlog is growing unbounded

**Symptom**: `totalPending` in the health indicator climbs and never
falls; the claim rate
(`rate(event_outboxer_events_queue_time_seconds_count[...])`) is flat.

**Diagnose**:
- Check the `event_outboxer.events.published` rate vs the processed
  rate (`event_outboxer_events_processing_time_seconds_count`) per
  `event_type`. If `published > processed`, handlers are the
  bottleneck.
- Are the pollers running? Check the engine logs for `poller start:
  eventType=…` once per type at startup.
- Is the handler pool saturated?
  `event_outboxer.events.retry_scheduled{reason="dispatch_rejected"}`
  non-zero confirms it — raise
  `event-outboxer.event-types.defaults.handler-pool-size`
  or `handler-queue-capacity`.
- Is the claim query itself slow? A rising
  `event_outboxer.poller.claim_time` average points at the database
  (index bloat, lock contention), not the handlers.

**Fix**: scale the handler pool for the affected type, or add another
replica. Per-type isolation means fixing one type does not help
another — tune each hot type separately.

### 2. Events accumulating in `DISABLED`

**Symptom**: the `event_outboxer.events.disabled` counter rising steadily and
`event_outboxer.events.backlog{status="disabled"}` never dropping.

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
  Redis lock. Look for the `event_outboxer.handler.stuck_time` count —
  if zero, the holder is alive but slow.
- Locker backend issue: `event_outboxer.lock.release_failed` non-zero hints
  at Redis / PG connectivity trouble.

**Fix**: if the bottleneck is an aggregate hot-spot, partition the
lock key more finely (include a sub-aggregate), or let the handler
thread wait for the key instead of releasing the event to the back of
the backlog: set the type's `lock-wait` (ADR-0035) to about the
holders' hold time and watch
`event_outboxer.lock.wait_time{outcome="acquired"}` — the busy counter
should fall and most acquisitions should complete well inside the
budget. If the locker backend is flaky, switch the adapter or add
backoff. Redis TTL will eventually release a stuck lock even if
`LockHandle.close()` never runs.

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
- If liveness is in `event-outboxer.health.probe-groups`, k8s restarts the
  pod automatically on the next probe cycle.
- Without liveness integration, restart the pod manually or let the
  Prometheus alert rule (`engine_state{state="running"}==0 for 1m`)
  escalate.
- Post-mortem: fix the underlying cause — heap sizing, stricter
  validation on the handler's input, etc. The engine does not
  attempt to restart itself in-process.

### 7. One event type stopped processing (leaked handler threads)

**Symptom**: `event_outboxer.handler.abandoned_threads{event_type=…}`
is non-zero and climbing; `handler.executor.free_capacity` for that
type sits at `0` while other types keep flowing;
`events.backlog{status="pending"}` grows for that type only; logs
contain `handler ignored the interrupt and did not yield …` (or, for a
type with `interrupt-stuck-handler: false`, `handler still running …
after force-reclaim`). The engine is `UP` — this is not a crash.

**Diagnose**:
- Each increment of `handler.abandoned` is one dispatch whose row was
  force-reclaimed and whose thread kept running anyway. With
  `interrupt-stuck-handler` on (the default) that means the interrupt
  was ignored — almost always blocking I/O with no timeout: an HTTP
  client with no connect/read timeout, a driver call with no socket
  timeout, a `take()` on a queue nobody feeds. With the opt-out on
  (`HandlerAbandonedInfo.interrupted=false`, logged at WARN) the thread
  was never asked to stop, so the reading is "this handler is slower
  than `handler-max-runtime`", not "this handler is unstoppable".
- `HandlerAbandonedInfo.threadName` names the thread — take a thread
  dump (`jcmd <pid> Thread.print`) and look at that thread's stack to
  see exactly what it is blocked on.
- The events themselves are **safe**: every one of them went back to
  `PENDING` with `attempts + 1` at reclaim time. Only this JVM's
  threads are lost.

**Fix**:
- Set timeouts on the client the handler calls. That is the actual fix;
  everything else is mitigation.
- Until then, `handler-pool-size` can be raised to buy time (each leak
  costs one slot), and `handler-max-runtime` should be set close to the
  handler's realistic worst case so the reclaim happens promptly.
- If the handler is deliberately non-interruptible, keep
  `interrupt-stuck-handler: false` for that type and treat the gauge as
  the leak budget.
- Restarting the pod is the only way to reclaim already-lost threads.

---

## Related documents

- [docs/ARCHITECTURE.md](ARCHITECTURE.md) — engine design and data flows.
- [docs/STORAGE.md](STORAGE.md) — SQL schema and operational queries.
- [docs/CONFIGURATION.md](CONFIGURATION.md) — every tunable property.
- [docs/TESTING.md](TESTING.md) — asserting over the same signals in
  your own tests via `event-outboxer-testkit`.
