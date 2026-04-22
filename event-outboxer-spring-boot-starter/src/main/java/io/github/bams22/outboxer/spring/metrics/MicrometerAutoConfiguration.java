/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.metrics;

import io.github.bams22.outboxer.api.handle.EventHandler;
import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.metrics.micrometer.MicrometerOutboxListener;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.EventStore;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.ToDoubleFunction;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Registers a {@link MicrometerOutboxListener} when both the metrics adapter and a Micrometer
 * {@link MeterRegistry} are on the classpath, plus per-state gauges for the engine lifecycle. The
 * metric-name prefix is bound from {@code outbox.metrics.prefix} (default:
 * {@code event_outboxer}).
 */
@AutoConfiguration
@ConditionalOnClass({MeterRegistry.class, MicrometerOutboxListener.class})
@ConditionalOnBean(MeterRegistry.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class MicrometerAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(MicrometerOutboxListener.class)
  public OutboxListener outboxMicrometerListener(
      MeterRegistry registry, OutboxProperties properties) {
    return new MicrometerOutboxListener(registry, properties.getMetrics().getPrefix());
  }

  /**
   * Publishes three gauges for {@link OutboxEngine#state()}: one per enum value, each either 0
   * or 1. Lets Prometheus users write alerts without remembering a numeric mapping, e.g.
   * {@code event_outboxer_engine_state{state="running"} == 0 for 1m}.
   *
   * <p>The meter is registered eagerly at context refresh — it shows
   * {@code state="stopped"=1} until {@code SmartLifecycle.start()} runs, then flips to
   * {@code state="running"=1}, then (on shutdown) to {@code state="stopping"=1} and back to
   * {@code state="stopped"=1}.
   */
  @Bean
  @ConditionalOnBean(OutboxEngine.class)
  @ConditionalOnMissingBean(name = "outboxEngineStateGauges")
  public OutboxEngineStateGauges outboxEngineStateGauges(
      MeterRegistry registry, OutboxEngine engine, OutboxProperties properties) {
    String name = properties.getMetrics().getPrefix() + ".engine.state";
    for (OutboxEngine.State value : OutboxEngine.State.values()) {
      Gauge.builder(name, engine, e -> e.state() == value ? 1.0 : 0.0)
          .tag("state", value.name().toLowerCase(Locale.ROOT))
          .description("Outbox engine lifecycle state indicator (0 or 1 per value)")
          .strongReference(true)
          .register(registry);
    }
    return new OutboxEngineStateGauges();
  }

  /** Marker bean so the eager registration above participates in DI. */
  public static final class OutboxEngineStateGauges {}

  /**
   * Publishes backlog gauges driven by {@link EventStore#metricsSnapshot()}. Every Prometheus
   * scrape reads the snapshot through the {@code MetricsSnapshotCache} SPI, so the cost per
   * scrape is amortised across the cache TTL (default 30 s) and — when
   * {@code outbox.cache.type=redis} — shared across pods so dashboards see the same aggregate
   * on every replica.
   *
   * <p>Per-event-type (one row per registered {@link EventHandler}):
   *
   * <ul>
   *   <li>{@code event_outboxer.events.pending{event_type="…"}}
   *   <li>{@code event_outboxer.events.processing{event_type="…"}}
   *   <li>{@code event_outboxer.events.disabled{event_type="…"}}
   *   <li>{@code event_outboxer.events.oldest_pending_age_seconds{event_type="…"}} — how long
   *       the oldest PENDING row of this type has been waiting; {@code 0} when no rows pending.
   * </ul>
   *
   * <p>Global (snapshot-wide, no tag):
   *
   * <ul>
   *   <li>{@code event_outboxer.events.oldest_claimed_age_seconds} — how long the oldest
   *       in-flight PROCESSING row has been running; useful as a backlog / stuck-handler signal
   *       even before the watchdog trips.
   * </ul>
   *
   * <p>To aggregate totals in PromQL: {@code sum without(event_type)(event_outboxer_events_pending)}.
   */
  @Bean
  @ConditionalOnBean({EventStore.class, OutboxEngine.class})
  @ConditionalOnMissingBean(name = "outboxBacklogGauges")
  public OutboxBacklogGauges outboxBacklogGauges(
      MeterRegistry registry,
      EventStore store,
      Clock clock,
      List<EventHandler<?>> handlers,
      OutboxProperties properties) {

    String prefix = properties.getMetrics().getPrefix() + ".events.";

    for (EventHandler<?> handler : handlers) {
      String eventType = handler.eventType();

      registerPerType(
          registry, prefix + "pending", eventType, store,
          s -> perType(s, eventType).map(OutboxMetricsSnapshot.EventTypeStats::pending).orElse(0L));
      registerPerType(
          registry, prefix + "processing", eventType, store,
          s -> perType(s, eventType).map(OutboxMetricsSnapshot.EventTypeStats::processing).orElse(0L));
      registerPerType(
          registry, prefix + "disabled", eventType, store,
          s -> perType(s, eventType).map(OutboxMetricsSnapshot.EventTypeStats::disabled).orElse(0L));

      Gauge.builder(
              prefix + "oldest_pending_age_seconds",
              store,
              s -> ageSeconds(
                  perType(s.metricsSnapshot(), eventType)
                      .map(OutboxMetricsSnapshot.EventTypeStats::oldestPendingRunAt)
                      .orElse(null),
                  clock.now()))
          .tag("event_type", eventType)
          .description(
              "Seconds since the oldest PENDING event of this type became eligible; 0 when empty")
          .strongReference(true)
          .register(registry);
    }

    Gauge.builder(
            prefix + "oldest_claimed_age_seconds",
            store,
            s -> ageSeconds(s.metricsSnapshot().oldestClaimedAt(), clock.now()))
        .description(
            "Seconds since the oldest PROCESSING event was claimed; 0 when nothing in-flight")
        .strongReference(true)
        .register(registry);

    return new OutboxBacklogGauges();
  }

  private static void registerPerType(
      MeterRegistry registry,
      String name,
      String eventType,
      EventStore store,
      ToDoubleFunction<OutboxMetricsSnapshot> extractor) {
    Gauge.builder(name, store, s -> extractor.applyAsDouble(s.metricsSnapshot()))
        .tag("event_type", eventType)
        .strongReference(true)
        .register(registry);
  }

  private static Optional<OutboxMetricsSnapshot.EventTypeStats> perType(
      OutboxMetricsSnapshot snapshot, String eventType) {
    for (OutboxMetricsSnapshot.EventTypeStats stats : snapshot.perType()) {
      if (stats.eventType().equals(eventType)) {
        return Optional.of(stats);
      }
    }
    return Optional.empty();
  }

  private static double ageSeconds(Instant from, Instant now) {
    if (from == null) {
      return 0.0;
    }
    long seconds = Duration.between(from, now).getSeconds();
    return seconds < 0 ? 0.0 : (double) seconds;
  }

  /** Marker bean for the backlog gauges registration. */
  public static final class OutboxBacklogGauges {}
}
