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
 * Auto-configuration for the {@link MetricsSnapshotCache} SPI that backs {@code
 * EventStore.metricsSnapshot()} caching.
 *
 * <p>Selected via {@code outbox.cache.type}:
 *
 * <ul>
 *   <li>{@code memory} (default) — per-JVM TTL cache, driven by {@code
 *       outbox.storage.metrics-cache-ttl}.
 *   <li>{@code noop} — caching disabled; every call recomputes from the database.
 *   <li>{@code redis} — shared Redis-backed cache; wired by {@code RedisCacheAutoConfiguration}
 *       when {@code event-outboxer-cache-redis} is on the classpath and a {@code
 *       StatefulRedisConnection} bean is available.
 * </ul>
 *
 * <p>All three options respect {@code @ConditionalOnMissingBean(MetricsSnapshotCache.class)} so a
 * user-defined {@code @Bean MetricsSnapshotCache} overrides the starter default regardless of the
 * selected type.
 */
@AutoConfiguration
@ConditionalOnProperty(
    prefix = "event-outboxer",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class CacheAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(MetricsSnapshotCache.class)
  @ConditionalOnProperty(
      prefix = "event-outboxer.cache",
      name = "type",
      havingValue = "memory",
      matchIfMissing = true)
  public MetricsSnapshotCache outboxInMemoryMetricsSnapshotCache(
      Clock clock, OutboxProperties properties) {
    return MetricsSnapshotCache.inMemory(clock, properties.getStorage().getMetricsCacheTtl());
  }

  @Bean
  @ConditionalOnMissingBean(MetricsSnapshotCache.class)
  @ConditionalOnProperty(prefix = "event-outboxer.cache", name = "type", havingValue = "noop")
  public MetricsSnapshotCache outboxNoopMetricsSnapshotCache() {
    return MetricsSnapshotCache.noop();
  }
}
