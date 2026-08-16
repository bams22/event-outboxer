/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.lock;

import io.github.bams22.outboxer.lock.redis.RedisEntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker;
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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

/**
 * Registers {@link RedisEntityLocker} when {@code event-outboxer.lock.type=redis}. The connection
 * comes from {@link OutboxRedisConnectionResolver} (ADR-0027): the
 * {@code @OutboxRedisConnection}-qualified bean, else the unique/{@code @Primary} one, else the
 * connection the starter created from {@code event-outboxer.redis.*}. With no connection resolvable
 * at all, startup fails fast with an actionable diagnosis — {@code lock.type=redis} is an explicit
 * opt-in, so silent back-off would only surface later as a cryptic missing-{@code EntityLocker}
 * error.
 */
@AutoConfiguration(after = RedisConnectionAutoConfiguration.class)
@ConditionalOnClass(RedisEntityLocker.class)
@Conditional(OnRedisLockCondition.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class RedisLockAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(EntityLocker.class)
    public EntityLocker outboxEntityLocker(
            @OutboxRedisConnection
                    ObjectProvider<StatefulRedisConnection<String, String>> qualified,
            ObjectProvider<StatefulRedisConnection<String, String>> connections,
            ListableBeanFactory beanFactory,
            OutboxProperties properties) {
        return new RedisEntityLocker(
                OutboxRedisConnectionResolver.resolve(qualified, connections, beanFactory),
                properties.getLock().getKeyPrefix());
    }
}
