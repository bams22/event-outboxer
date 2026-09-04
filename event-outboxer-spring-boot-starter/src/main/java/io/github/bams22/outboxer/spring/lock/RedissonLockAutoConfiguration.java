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

import io.github.bams22.outboxer.lock.redisson.RedissonEntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spring.OutboxProperties;
import io.github.bams22.outboxer.spring.OutboxRedissonClient;
import java.util.List;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

/**
 * Registers {@link RedissonEntityLocker} when {@code event-outboxer.lock.type=redisson} (ADR-0036).
 * The client is the application's: the {@code @OutboxRedissonClient}-qualified {@code
 * RedissonClient} bean, else the unique/{@code @Primary} one, else startup fails fast naming the
 * candidates — the starter never creates a Redisson client, and {@code redisson} is an explicit
 * opt-in, so silent back-off would only surface later as a cryptic missing-{@code EntityLocker}
 * error.
 */
@AutoConfiguration
@ConditionalOnClass({RedissonEntityLocker.class, RedissonClient.class})
@Conditional(OnRedissonLockCondition.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class RedissonLockAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(EntityLocker.class)
    public EntityLocker outboxEntityLocker(
            @OutboxRedissonClient ObjectProvider<RedissonClient> qualified,
            ObjectProvider<RedissonClient> clients,
            ListableBeanFactory beanFactory,
            OutboxProperties properties) {
        OutboxProperties.Lock lock = properties.getLock();
        return RedissonEntityLocker.builder()
                .client(resolve(qualified, clients, beanFactory))
                .keyPrefix(lock.getKeyPrefix())
                .fair(lock.isFair())
                .nativeWait(lock.isWakeup())
                .build();
    }

    static RedissonClient resolve(
            ObjectProvider<RedissonClient> qualified,
            ObjectProvider<RedissonClient> all,
            ListableBeanFactory beanFactory) {
        RedissonClient qualifiedBean;
        try {
            qualifiedBean = qualified.getIfAvailable();
        } catch (NoUniqueBeanDefinitionException ex) {
            throw new IllegalStateException(
                    "event-outboxer.lock.type=redisson: several RedissonClient beans carry"
                            + " @OutboxRedissonClient ("
                            + ex.getBeanNamesFound()
                            + ") — mark exactly one.",
                    ex);
        }
        if (qualifiedBean != null) {
            return qualifiedBean;
        }
        RedissonClient unique = all.getIfUnique();
        if (unique != null) {
            return unique;
        }
        List<String> candidates =
                List.of(beanFactory.getBeanNamesForType(RedissonClient.class, true, false));
        if (candidates.isEmpty()) {
            throw new IllegalStateException(
                    "event-outboxer.lock.type=redisson needs a RedissonClient bean, and none is"
                            + " defined. The starter does not create one: add"
                            + " redisson-spring-boot-starter or define a RedissonClient bean"
                            + " (optionally marked with"
                            + " io.github.bams22.outboxer.spring.@OutboxRedissonClient).");
        }
        throw new IllegalStateException(
                "event-outboxer.lock.type=redisson: several RedissonClient beans and none is"
                        + " @Primary or marked with @OutboxRedissonClient: "
                        + candidates
                        + ". Mark the outbox one with"
                        + " io.github.bams22.outboxer.spring.@OutboxRedissonClient.");
    }
}
