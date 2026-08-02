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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.bams22.outboxer.cache.redis.LettuceMetricsSnapshotCache;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Registers {@link LettuceMetricsSnapshotCache} when {@code event-outboxer.cache.type=redis}, the
 * {@code event-outboxer-cache-redis} module is on the classpath, and a {@link
 * StatefulRedisConnection} bean is available (users create one via Lettuce's {@code RedisClient}).
 *
 * <p>TTL comes from {@code event-outboxer.storage.metrics-cache-ttl}; the key prefix from {@code
 * event-outboxer.cache.redis.key-prefix} (default {@code outbox:metrics:}).
 */
@AutoConfiguration
@ConditionalOnClass(LettuceMetricsSnapshotCache.class)
@ConditionalOnBean(StatefulRedisConnection.class)
@ConditionalOnProperty(prefix = "event-outboxer.cache", name = "type", havingValue = "redis")
public class RedisCacheAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(MetricsSnapshotCache.class)
  public MetricsSnapshotCache outboxRedisMetricsSnapshotCache(
      StatefulRedisConnection<String, String> connection, OutboxProperties properties) {
    return new LettuceMetricsSnapshotCache(
        connection,
        properties.getStorage().getMetricsCacheTtl(),
        properties.getCache().getRedis().getKeyPrefix(),
        JsonMapper.builder().addModule(new JavaTimeModule()).build());
  }
}
