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
import io.github.bams22.outboxer.spring.lock.OnRedisLockCondition;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
 *
 * <p>With {@code event-outboxer.lock.type=redis} and {@code event-outboxer.lock.wakeup=true} (the
 * default) a second, pub/sub connection {@code outboxRedisPubSubConnection} is opened on the same
 * client for the locker's release notifications (ADR-0035). It carries no qualifier: the command
 * connection stays the one {@code @OutboxRedisConnection} bean, and the locker resolves the pub/sub
 * connection by its own type.
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

    @Bean(destroyMethod = "")
    @Conditional(OnRedisLockCondition.class)
    @ConditionalOnProperty(
            name = "event-outboxer.lock.wakeup",
            havingValue = "true",
            matchIfMissing = true)
    public StatefulRedisPubSubConnection<String, String> outboxRedisPubSubConnection(
            OutboxLettuceConnectionManager manager) {
        return manager.getPubSubConnection();
    }
}
