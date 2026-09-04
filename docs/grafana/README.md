# Grafana dashboard

Ready-to-import operational dashboard for event-outboxer:
[`event-outboxer-dashboard.json`](event-outboxer-dashboard.json).

## Importing

1. Grafana → **Dashboards → New → Import**.
2. Paste the contents of `event-outboxer-dashboard.json` (or upload the
   file) and click **Load → Import**.
3. Pick your Prometheus datasource in the **Datasource** variable at the
   top of the dashboard (defaults to the first Prometheus datasource).

No manual datasource mapping is required — the dashboard uses a
`DS_PROMETHEUS` template variable instead of hardcoded datasource UIDs.

## Expected labels

Every query filters on four labels:

| Variable | Label | Where it comes from |
|---|---|---|
| Environment | `environment` | your scrape config / external labels |
| Service | `service` | your scrape config / external labels |
| Pod | `pod` (multi, All) | Kubernetes service discovery relabeling |
| Event type | `event_type` (multi, All) | emitted by the library on every per-event meter |

`environment`, `service` and `pod` are **not** produced by the library —
they must be attached by your Prometheus scrape configuration (the
standard `kube-prometheus-stack` relabeling provides `pod`; `service` and
`environment` are typically external or relabeled labels). If your
installation uses different label names (e.g. `app` instead of
`service`, `env` instead of `environment`), search-and-replace them in
the JSON before importing.

## Aggregation semantics baked into the queries

- **Store-wide gauges** — `events.backlog`, `oldest_pending_age_seconds`,
  `oldest_claimed_age_seconds`, `entity_locks.held` — read one snapshot
  of the whole database, so every pod reports the same value. The
  dashboard aggregates them across pods with `max`, never `sum` (sum
  would multiply the real value by the pod count).
- **Per-JVM metrics** — all counters, `events.in_flight`,
  `handler.executor.*` — are summed across pods;
  `heartbeat.last_success_age_seconds` is per-JVM too and is graphed
  per pod / aggregated with `max` (a sum of ages is meaningless).
- **Entity locks row** (ADR-0035): lock wait p99/p50 by outcome and
  hold time p99/avg by event type come from the `lock.wait_time` and
  `lock.hold_time` histograms; the "waits exhausted" share is
  `busy / (acquired + busy)` over the wait-time counts; "wake-ups by
  result" (`lock.wakeups`, Redis locker with the pub/sub wake-up only)
  is the one panel that shows a silently broken pub/sub path —
  `probed` outgrowing `notified`. Per-key detail is deliberately not a
  metric; it lives on the consumer span (`event_outboxer.lock.key`,
  `event_outboxer.lock.wait_ms`).

## Requirements

- **Micrometer Prometheus registry** (the standard
  `micrometer-registry-prometheus` + Spring Boot Actuator setup). Metric
  names in the queries follow Micrometer's Prometheus naming:
  `event_outboxer_events_published_total`,
  `event_outboxer_events_queue_time_seconds_bucket`, etc.
- **Latency panels work out of the box.** The Spring Boot starter
  automatically applies default SLO histogram buckets (10ms–1h for queue
  time, 10ms–10m for processing time, 1ms–5s for the lock wait, 1ms–10m
  for the lock hold) to `events.queue_time`, `events.processing_time`,
  `lock.wait_time` and `lock.hold_time`, so the `histogram_quantile`
  p99/p50 panels have data with zero configuration; quantile precision
  is limited to the bucket grid.
  For finer buckets enable full percentile histograms on top:

  ```yaml
  management:
    metrics:
      distribution:
        percentiles-histogram:
          event_outboxer.events.queue_time: true
          event_outboxer.events.processing_time: true
  ```

  The avg/max latency panels work regardless of histogram settings.
  If the application disables the shipped defaults
  (`event-outboxer.metrics.distribution-defaults.enabled: false`)
  without configuring its own buckets, the p99/p50 panels stay empty.
- **Default metric prefix.** If you override
  `event-outboxer.metrics.prefix`, replace the `event_outboxer_` prefix
  in the JSON accordingly.
- The **entity-lock leases** panel has data only with
  `event-outboxer.lock.type: postgres-lease` (the default on
  PostgreSQL).

The full metric reference — every meter, its tags, when it fires and how
to interpret it — lives in [OBSERVABILITY.md](../OBSERVABILITY.md).
