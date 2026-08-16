/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.redis;

import io.github.bams22.outboxer.spring.OutboxProperties;
import io.github.bams22.outboxer.spring.OutboxRedisConnection;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

/**
 * Creates the starter-managed {@link StatefulRedisConnection} from {@code event-outboxer.redis.*}
 * (ADR-0027). Applies only when Lettuce is on the classpath, {@code uri} or {@code host} is set,
 * and the application defines no {@code StatefulRedisConnection} bean of its own — a user bean
 * always wins and renders these properties inert.
 *
 * <p>The connection carries {@link OutboxRedisConnection @OutboxRedisConnection} so the Redis
 * entity locker and Redis metrics cache resolve it deterministically even next to unrelated
 * connections. Lifecycle is owned by {@link OutboxLettuceConnectionManager}; the bean name {@code
 * outboxRedisConnection} is part of the documented contract.
 */
@AutoConfiguration
@ConditionalOnClass(RedisClient.class)
@Conditional(OnOutboxRedisConfiguredCondition.class)
@ConditionalOnMissingBean(StatefulRedisConnection.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class RedisConnectionAutoConfiguration {

    @Bean
    public OutboxLettuceConnectionManager outboxRedisConnectionManager(
            OutboxProperties properties) {
        return new OutboxLettuceConnectionManager(properties.getRedis());
    }

    @Bean(destroyMethod = "")
    @OutboxRedisConnection
    public StatefulRedisConnection<String, String> outboxRedisConnection(
            OutboxLettuceConnectionManager manager) {
        return manager.getConnection();
    }
}
