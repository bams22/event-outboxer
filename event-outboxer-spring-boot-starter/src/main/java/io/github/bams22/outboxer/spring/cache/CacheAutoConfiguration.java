/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.cache;

import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spring.OutboxProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for the {@link MetricsSnapshotCache} SPI that backs
 * {@code EventStore.metricsSnapshot()} caching.
 *
 * <p>Default is an in-memory TTL cache driven by {@code outbox.storage.metrics-cache-ttl}. Users
 * who want a shared cache across pods (so {@code /actuator/health/outbox} returns the same
 * snapshot on every replica) define their own {@code @Bean MetricsSnapshotCache}; the
 * {@code event-outboxer-cache-redis} module ships a ready-to-use Lettuce-backed implementation.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
public class CacheAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(MetricsSnapshotCache.class)
  public MetricsSnapshotCache outboxMetricsSnapshotCache(Clock clock, OutboxProperties properties) {
    return MetricsSnapshotCache.inMemory(clock, properties.getStorage().getMetricsCacheTtl());
  }
}
