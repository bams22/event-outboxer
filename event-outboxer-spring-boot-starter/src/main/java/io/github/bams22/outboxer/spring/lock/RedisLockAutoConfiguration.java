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
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.function.ToLongFunction;
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
import org.springframework.context.annotation.Configuration;

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
                properties.getLock().isWakeup()
                        ? OutboxRedisConnectionResolver.resolvePubSub(
                                qualifiedPubSub, pubSubConnections)
                        : null;
        if (properties.getLock().isWakeup() && wakeup == null) {
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

    /**
     * {@code <metrics.prefix>.lock.wakeups{result}} — how the Redis locker's bounded waits ended
     * (ADR-0035): {@code notified} (a release notification woke the waiter), {@code probed}
     * (acquired on a probe no notification preceded), {@code exhausted} (budget spent), {@code
     * interrupted}. The one signal that the pub/sub path works: a broken one shows every
     * acquisition under {@code probed}. Registered as a {@link MeterBinder} so Boot applies it to
     * every registry; absent without the wake-up connection.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({MeterRegistry.class, MeterBinder.class})
    static class WakeupMetricsConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "outboxLockWakeupMeters")
        public MeterBinder outboxLockWakeupMeters(
                ObjectProvider<EntityLocker> lockerProvider, OutboxProperties properties) {
            String name = properties.getMetrics().getPrefix() + ".lock.wakeups";
            return registry -> {
                if (!(lockerProvider.getIfAvailable() instanceof RedisEntityLocker locker)
                        || !locker.wakeupEnabled()) {
                    return;
                }
                register(registry, name, locker, "notified", s -> s.notified());
                register(registry, name, locker, "probed", s -> s.probed());
                register(registry, name, locker, "exhausted", s -> s.exhausted());
                register(registry, name, locker, "interrupted", s -> s.interrupted());
            };
        }

        private static void register(
                MeterRegistry registry,
                String name,
                RedisEntityLocker locker,
                String result,
                ToLongFunction<io.github.bams22.outboxer.spi.LockWaiters.WakeupStats> field) {
            FunctionCounter.builder(name, locker, l -> field.applyAsLong(l.wakeupStats()))
                    .tag("result", result)
                    .description("How the Redis locker's bounded lock waits ended (ADR-0035)")
                    .register(registry);
        }
    }
}
