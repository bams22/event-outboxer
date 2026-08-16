# event-outboxer-metrics-micrometer

An `OutboxListener` that publishes every engine signal to a Micrometer
`MeterRegistry` — counters, timers and distribution summaries prefixed
`event_outboxer.*`, with an `event_type` tag on every per-event signal.

| | |
|---|---|
| Coordinates | `io.github.bams22:event-outboxer-metrics-micrometer` |
| Java package | `io.github.bams22.outboxer.metrics.micrometer` |
| Depends on | [`event-outboxer-api`](event-outboxer-api.md) only (+ `micrometer-core`) |
| Enable with | module on the classpath + a `MeterRegistry` bean — no flag |

## Why it exists

The engine reports everything through the `OutboxListener` port
([ADR-0013](../adr/0013-outbox-listener-for-observability.md)); this
module is the standard bridge from those callbacks to Micrometer, so
any Spring Boot Actuator app gets outbox metrics in Prometheus /
Datadog / OTLP with zero glue code.

## What it does

**`MicrometerOutboxListener`** implements all 25 callbacks; each is an
O(1) meter update, safe on the dispatcher hot path. Highlights (full
catalogue in [OBSERVABILITY.md §Micrometer metrics reference](../OBSERVABILITY.md#micrometer-metrics-reference)):

| Meter (default prefix) | Type | Tag |
|---|---|---|
| `event_outboxer.events.published` / `.claimed` / `.processed` / `.deleted` / `.skipped` | counter | `event_type` |
| `event_outboxer.events.retry_scheduled` / `.disabled` | counter | `event_type`, `reason` (bounded trigger set) |
| `event_outboxer.events.processing_time`, `.events.queue_time` | timer | `event_type` |
| `event_outboxer.events.attempts` | distribution summary | `event_type` |
| `event_outboxer.handler.errors` | counter | `event_type`, `exception` |
| `event_outboxer.handler.stuck_reclaimed` | counter | `event_type` |
| `event_outboxer.handler.stuck_time` | timer | `event_type` |
| `event_outboxer.events.unknown_type`, `.events.serialization_errors` | counter | `event_type` |
| `event_outboxer.poller.polls` | counter | `event_type`, `result` (`claimed`/`empty`) |
| `event_outboxer.poller.batch_size` | distribution summary | `event_type` |
| `event_outboxer.poller.saturated` | counter | `event_type` |
| `event_outboxer.lock.acquisition_failed` | counter | `event_type`, `outcome` (`busy`/`error`) |
| `event_outboxer.lock.release_failed` | counter | `event_type` |
| `event_outboxer.dispatch.rejected` | counter | `event_type` |
| `event_outboxer.storage.errors` | counter | `operation` |
| `event_outboxer.retention.purged` | counter | `kind` (`archive`/`disabled`) |
| `event_outboxer.claims.stale_swept` | counter | — |
| `event_outboxer.workers.registered` / `.graceful_stops` / `.deregistered`, `.heartbeat.failed`, `.orphans.reclaimed`, `.orphans.dead_workers`, `.engine.crashed` | counter | — |

**Gauges live in the starter, not here.** When both this module and
the engine are present, the starter's `MicrometerAutoConfiguration`
additionally registers `event_outboxer.engine.state{state=…}` (0/1
per lifecycle state — the alerting signal for a crashed engine) and
the backlog gauge `event_outboxer.events.backlog{status=pending|processing|disabled}`
and `.oldest_pending_age_seconds` per handler type plus a
global `.oldest_claimed_age_seconds`, all read through the
`MetricsSnapshotCache` (see
[event-outboxer-cache-redis](event-outboxer-cache-redis.md) for
fleet-consistent readings).

## When to use it

Any Spring Boot app with Micrometer — effectively always in
production; ARTIFACTS.md lists it in the typical production set.
Plain-Java (non-Spring) apps can use it too: it depends only on the
API and `micrometer-core`.

## How to configure it

### With Spring Boot

```xml
<dependency>
    <groupId>io.github.bams22</groupId>
    <artifactId>event-outboxer-metrics-micrometer</artifactId>
</dependency>
```

That is all: the starter auto-registers the listener when the class
and a `MeterRegistry` bean are present. One property:

```yaml
event-outboxer:
  metrics:
    prefix: event_outboxer   # default; chosen to avoid clashing with other libraries' outbox.* metrics
```

Override the prefix when several outbox instances share one registry
or your organisation mandates a namespace. Declaring your own
`MicrometerOutboxListener` bean replaces the auto-configured one; the
gauges can be replaced individually by defining beans named
`outboxEngineStateGauges` / `outboxBacklogGauges` /
`outboxSaturationGauges`.

### Distribution defaults: SLO histogram buckets (applied automatically)

The module ships `META-INF/event-outboxer/metrics-defaults.yml` with
default SLO histogram buckets for every event-outboxer timer:
`events.queue_time` and `events.processing_time` get a 10ms–1m grid
(18 boundaries, dense in the sub-second range), `handler.stuck_time`
a 30s–1h grid. In a Spring Boot app the starter applies the file
automatically at startup (an `EnvironmentPostProcessor` appends it
with the lowest precedence), so fleet-wide
`histogram_quantile()` works in Prometheus with zero configuration —
the `_bucket{le=…}` series aggregate across pods, unlike client-side
percentiles.

Two knobs:

- any `management.metrics.distribution.*` value you set yourself
  always wins over these defaults (an SLO list for the same meter
  replaces the shipped grid entirely);
- opt out completely with
  `event-outboxer.metrics.distribution-defaults.enabled: false`.

Quantile precision is limited to the bucket grid; for finer buckets
enable `management.metrics.distribution.percentiles-histogram` on top
(the boundaries merge). The keys inside the file use the default
metric prefix — if you override `event-outboxer.metrics.prefix`, the
defaults simply stop matching and you set your own boundaries. Plain
Java (non-Spring) apps are unaffected — configure a
`DistributionStatisticConfig` via `MeterFilter` yourself.

### Without Spring

```java
new OutboxEngineBuilder()
    .listener(new MicrometerOutboxListener(meterRegistry))          // default prefix
    // or: new MicrometerOutboxListener(meterRegistry, "my_prefix")
    .build();
```

Backlog gauges are not included — register your own `Gauge.builder`
readings off `EventStore.metricsSnapshot()` if you need them.

## Related

- [OBSERVABILITY.md](../OBSERVABILITY.md) — the full metric table, alerting recipes and the troubleshooting playbook.
- [event-outboxer-tracing-otel](event-outboxer-tracing-otel.md) / [-micrometer](event-outboxer-tracing-micrometer.md) — the tracing side of observability.
