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

import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.core.engine.OutboxEngine;
import io.github.bams22.outboxer.metrics.micrometer.MicrometerOutboxListener;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;
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
}
