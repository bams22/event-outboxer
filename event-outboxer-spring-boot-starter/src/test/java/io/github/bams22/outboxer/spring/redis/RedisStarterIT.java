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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.cache.redis.LettuceMetricsSnapshotCache;
import io.github.bams22.outboxer.lock.redis.RedisEntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spring.cache.RedisCacheAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.RedisLockAutoConfiguration;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end check of the property-driven starter-managed Redis connection (ADR-0027) against a
 * real server: {@code event-outboxer.redis.*} alone yields a working {@link RedisEntityLocker} and
 * {@link LettuceMetricsSnapshotCache} sharing one connection, and the starter closes that
 * connection on context shutdown.
 */
@Testcontainers
class RedisStarterIT {

    @Container
    static GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(
                        AutoConfigurations.of(
                                RedisConnectionAutoConfiguration.class,
                                RedisLockAutoConfiguration.class,
                                RedisCacheAutoConfiguration.class));
    }

    @Test
    void hostPortPropertiesWireLockerAndCacheOverOneManagedConnection() {
        StatefulRedisConnection<?, ?>[] captured = new StatefulRedisConnection<?, ?>[1];
        runner().withPropertyValues(
                        "event-outboxer.redis.host=" + REDIS.getHost(),
                        "event-outboxer.redis.port=" + REDIS.getMappedPort(6379),
                        "event-outboxer.lock.type=redis",
                        "event-outboxer.cache.type=redis")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            EntityLocker locker = ctx.getBean(EntityLocker.class);
                            assertThat(locker).isInstanceOf(RedisEntityLocker.class);
                            assertThat(ctx.getBean(MetricsSnapshotCache.class))
                                    .isInstanceOf(LettuceMetricsSnapshotCache.class);

                            @SuppressWarnings("unchecked")
                            StatefulRedisConnection<String, String> connection =
                                    ctx.getBean(
                                            "outboxRedisConnection", StatefulRedisConnection.class);
                            captured[0] = connection;

                            try (var handle =
                                    locker.tryLock("order-42", Duration.ofSeconds(30))
                                            .orElseThrow()) {
                                assertThat(connection.sync().keys("outbox:lock:order-42"))
                                        .hasSize(1);
                            }
                            assertThat(connection.sync().keys("outbox:lock:order-42")).isEmpty();
                        });
        // The runner closed the context; the manager must have closed its connection.
        assertThat(captured[0].isOpen()).isFalse();
    }

    @Test
    void lockTypeRedisOpensAPubSubConnectionForTheWakeupAndClosesItWithTheContext() {
        StatefulRedisPubSubConnection<?, ?>[] captured = new StatefulRedisPubSubConnection<?, ?>[1];
        runner().withPropertyValues(
                        "event-outboxer.redis.host=" + REDIS.getHost(),
                        "event-outboxer.redis.port=" + REDIS.getMappedPort(6379),
                        "event-outboxer.lock.type=redis")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            RedisEntityLocker locker =
                                    (RedisEntityLocker) ctx.getBean(EntityLocker.class);
                            assertThat(locker.wakeupEnabled()).isTrue();
                            assertThat(ctx).hasBean("outboxRedisPubSubConnection");
                            captured[0] =
                                    ctx.getBean(
                                            "outboxRedisPubSubConnection",
                                            StatefulRedisPubSubConnection.class);
                            // The command connection is still the one qualified bean.
                            assertThat(ctx.getBean("outboxRedisConnection"))
                                    .isNotInstanceOf(StatefulRedisPubSubConnection.class);
                            // A waiter really parks on the notification: hold, release, acquire.
                            try (var handle =
                                    locker.tryLock("wake-42", Duration.ofSeconds(30))
                                            .orElseThrow()) {
                                assertThat(
                                                locker.tryLock(
                                                        "wake-42",
                                                        Duration.ofSeconds(30),
                                                        Duration.ofMillis(50)))
                                        .isEmpty();
                            }
                            assertThat(
                                            locker.tryLock(
                                                    "wake-42",
                                                    Duration.ofSeconds(30),
                                                    Duration.ofMillis(50)))
                                    .isPresent();
                        });
        assertThat(captured[0].isOpen()).isFalse();
    }

    @Test
    void wakeupOffKeepsPollingAndOpensNoPubSubConnection() {
        runner().withPropertyValues(
                        "event-outboxer.redis.host=" + REDIS.getHost(),
                        "event-outboxer.redis.port=" + REDIS.getMappedPort(6379),
                        "event-outboxer.lock.type=redis",
                        "event-outboxer.lock.wakeup=false")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean("outboxRedisPubSubConnection");
                            RedisEntityLocker locker =
                                    (RedisEntityLocker) ctx.getBean(EntityLocker.class);
                            assertThat(locker.wakeupEnabled()).isFalse();
                        });
    }

    @Test
    void cacheOnlyOpensNoPubSubConnection() {
        runner().withPropertyValues(
                        "event-outboxer.redis.host=" + REDIS.getHost(),
                        "event-outboxer.redis.port=" + REDIS.getMappedPort(6379),
                        "event-outboxer.cache.type=redis")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean("outboxRedisPubSubConnection");
                        });
    }

    @Test
    void uriPropertyWinsAndWiresTheLocker() {
        runner().withPropertyValues(
                        "event-outboxer.redis.uri=redis://"
                                + REDIS.getHost()
                                + ":"
                                + REDIS.getMappedPort(6379),
                        // Deliberately bogus discrete fields: uri must win.
                        "event-outboxer.redis.host=unreachable.invalid",
                        "event-outboxer.redis.port=1",
                        "event-outboxer.lock.type=redis")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            EntityLocker locker = ctx.getBean(EntityLocker.class);
                            assertThat(locker.tryLock("uri-check", Duration.ofSeconds(5)))
                                    .isPresent();
                        });
    }
}
