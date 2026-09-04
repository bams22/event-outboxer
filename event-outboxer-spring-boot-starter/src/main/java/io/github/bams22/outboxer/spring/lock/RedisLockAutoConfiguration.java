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
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 *
 * <p>The pub/sub connection for the bounded wait's wake-up (ADR-0035) is optional: with {@code
 * event-outboxer.lock.wakeup=true} (default) the locker takes the qualified or unique {@code
 * StatefulRedisPubSubConnection} bean — the starter opens one next to its command connection — and
 * logs which mode it runs in; without one it polls.
 */
@AutoConfiguration(after = RedisConnectionAutoConfiguration.class)
@ConditionalOnClass(RedisEntityLocker.class)
@Conditional(OnRedisLockCondition.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class RedisLockAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(RedisLockAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(EntityLocker.class)
    public EntityLocker outboxEntityLocker(
            @OutboxRedisConnection
                    ObjectProvider<StatefulRedisConnection<String, String>> qualified,
            ObjectProvider<StatefulRedisConnection<String, String>> connections,
            @OutboxRedisConnection
                    ObjectProvider<StatefulRedisPubSubConnection<String, String>> qualifiedPubSub,
            ObjectProvider<StatefulRedisPubSubConnection<String, String>> pubSubConnections,
            ListableBeanFactory beanFactory,
            OutboxProperties properties) {
        StatefulRedisPubSubConnection<String, String> wakeup =
                properties.getLock().wakeupOr(true)
                        ? OutboxRedisConnectionResolver.resolvePubSub(
                                qualifiedPubSub, pubSubConnections)
                        : null;
        if (properties.getLock().wakeupOr(true) && wakeup == null) {
            log.info(
                    "Redis entity locker: no StatefulRedisPubSubConnection bean — a busy lock is"
                            + " polled during lock-wait; define one (or set event-outboxer.redis.*)"
                            + " for release notifications");
        }
        return RedisEntityLocker.builder()
                .connection(
                        OutboxRedisConnectionResolver.resolve(qualified, connections, beanFactory))
                .keyPrefix(properties.getLock().getKeyPrefix())
                .wakeupConnection(wakeup)
                .build();
    }
}
