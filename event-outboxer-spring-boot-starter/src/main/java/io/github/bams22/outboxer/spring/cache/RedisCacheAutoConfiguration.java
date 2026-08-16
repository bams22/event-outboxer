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
import io.github.bams22.outboxer.spring.OutboxRedisConnection;
import io.github.bams22.outboxer.spring.redis.OutboxRedisConnectionResolver;
import io.github.bams22.outboxer.spring.redis.RedisConnectionAutoConfiguration;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Registers {@link LettuceMetricsSnapshotCache} when {@code event-outboxer.cache.type=redis} and
 * the {@code event-outboxer-cache-redis} module is on the classpath. The connection comes from
 * {@link OutboxRedisConnectionResolver} (ADR-0027) and is shared with the Redis entity locker; with
 * no connection resolvable at all, startup fails fast with an actionable diagnosis — {@code
 * cache.type=redis} is an explicit opt-in.
 *
 * <p>TTL comes from {@code event-outboxer.storage.metrics-cache-ttl}; the key prefix from {@code
 * event-outboxer.cache.redis.key-prefix} (default {@code outbox:metrics:}).
 */
@AutoConfiguration(after = RedisConnectionAutoConfiguration.class)
@ConditionalOnClass(LettuceMetricsSnapshotCache.class)
@ConditionalOnProperty(prefix = "event-outboxer.cache", name = "type", havingValue = "redis")
@EnableConfigurationProperties(OutboxProperties.class)
public class RedisCacheAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(MetricsSnapshotCache.class)
    public MetricsSnapshotCache outboxRedisMetricsSnapshotCache(
            @OutboxRedisConnection
                    ObjectProvider<StatefulRedisConnection<String, String>> qualified,
            ObjectProvider<StatefulRedisConnection<String, String>> connections,
            ListableBeanFactory beanFactory,
            OutboxProperties properties) {
        return new LettuceMetricsSnapshotCache(
                OutboxRedisConnectionResolver.resolve(qualified, connections, beanFactory),
                properties.getStorage().getMetricsCacheTtl(),
                properties.getCache().getRedis().getKeyPrefix(),
                JsonMapper.builder().addModule(new JavaTimeModule()).build());
    }
}
