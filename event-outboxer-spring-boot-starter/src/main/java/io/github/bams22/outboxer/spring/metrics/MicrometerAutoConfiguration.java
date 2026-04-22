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
import io.github.bams22.outboxer.metrics.micrometer.MicrometerOutboxListener;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Registers a {@link MicrometerOutboxListener} when both the metrics adapter and a Micrometer
 * {@link MeterRegistry} are on the classpath. The metric-name prefix is bound from
 * {@code outbox.metrics.prefix} (default: {@code event_outboxer}).
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
}
