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
import io.github.bams22.outboxer.spring.OutboxRedisConnection;
import io.github.bams22.outboxer.spring.cache.RedisCacheAutoConfiguration;
import io.github.bams22.outboxer.spring.lock.RedisLockAutoConfiguration;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.diagnostics.FailureAnalysis;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Redis connection selection per ADR-0027: the {@code @OutboxRedisConnection}-qualified bean wins
 * (even over an unrelated {@code @Primary}), otherwise the unique/{@code @Primary} bean, otherwise
 * startup fails fast naming the candidates — and with no connection at all a Redis-backed feature
 * fails fast pointing at {@code event-outboxer.redis.*} instead of dying later with a cryptic
 * missing-{@code EntityLocker} error.
 */
class OutboxRedisConnectionSelectionTest {

    private final ApplicationContextRunner strictRunner =
            new ApplicationContextRunner().withUserConfiguration(StrictCaptureConfiguration.class);

    // -------------------------------------------------------------------------------------------
    // resolver semantics
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("the @OutboxRedisConnection-qualified bean wins over a plain one")
    void qualifiedBeanWins() {
        strictRunner
                .withUserConfiguration(QualifiedPlusPlainConfiguration.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(ResolvedHolder.class).connection)
                                    .isSameAs(ctx.getBean("outboxRedis"));
                        });
    }

    @Test
    @DisplayName("the qualified bean wins even when another bean is @Primary")
    void qualifiedBeatsPrimary() {
        strictRunner
                .withUserConfiguration(QualifiedVsPrimaryConfiguration.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(ResolvedHolder.class).connection)
                                    .isSameAs(ctx.getBean("outboxRedis"));
                        });
    }

    @Test
    @DisplayName("a single unmarked connection resolves as-is")
    void singleBeanResolves() {
        strictRunner
                .withUserConfiguration(SingleConnectionConfiguration.class)
                .run(
                        ctx ->
                                assertThat(ctx.getBean(ResolvedHolder.class).connection)
                                        .isSameAs(ctx.getBean("onlyRedis")));
    }

    @Test
    @DisplayName("with several beans and no qualifier, @Primary wins")
    void primaryWins() {
        strictRunner
                .withUserConfiguration(PrimaryPlusPlainConfiguration.class)
                .run(
                        ctx ->
                                assertThat(ctx.getBean(ResolvedHolder.class).connection)
                                        .isSameAs(ctx.getBean("primaryRedis")));
    }

    @Test
    @DisplayName("several beans, none primary or qualified — startup fails naming the candidates")
    void ambiguousFailsFast() {
        strictRunner
                .withUserConfiguration(TwoPlainConnectionsConfiguration.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasStackTraceContaining("@OutboxRedisConnection")
                                    .hasStackTraceContaining("redisA")
                                    .hasStackTraceContaining("redisB");
                        });
    }

    @Test
    @DisplayName("two beans both carrying @OutboxRedisConnection — startup fails with 'exactly'")
    void twoQualifiedFailFast() {
        strictRunner
                .withUserConfiguration(TwoQualifiedConfiguration.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasStackTraceContaining("exactly one");
                        });
    }

    @Test
    @DisplayName("no connection at all — startup fails pointing at event-outboxer.redis.*")
    void noneAvailableFailsFast() {
        strictRunner.run(
                ctx -> {
                    assertThat(ctx).hasFailed();
                    assertThat(ctx.getStartupFailure())
                            .hasStackTraceContaining("event-outboxer.redis.uri");
                });
    }

    // -------------------------------------------------------------------------------------------
    // auto-configuration wiring
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("Redis locker boots against the qualified connection among several")
    void lockerUsesQualifiedConnection() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisLockAutoConfiguration.class))
                .withUserConfiguration(QualifiedPlusPlainConfiguration.class)
                .withPropertyValues("event-outboxer.lock.type=redis")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(EntityLocker.class))
                                    .isInstanceOf(RedisEntityLocker.class);
                        });
    }

    @Test
    @DisplayName("Redis metrics cache boots against the qualified connection among several")
    void cacheUsesQualifiedConnection() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisCacheAutoConfiguration.class))
                .withUserConfiguration(QualifiedPlusPlainConfiguration.class)
                .withPropertyValues("event-outboxer.cache.type=redis")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(MetricsSnapshotCache.class))
                                    .isInstanceOf(LettuceMetricsSnapshotCache.class);
                        });
    }

    @Test
    @DisplayName("lock.type=redis with no connection and no properties fails fast, diagnosed")
    void lockTypeRedisWithNothingFailsFast() {
        new ApplicationContextRunner()
                .withConfiguration(
                        AutoConfigurations.of(
                                RedisConnectionAutoConfiguration.class,
                                RedisLockAutoConfiguration.class))
                .withPropertyValues("event-outboxer.lock.type=redis")
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasStackTraceContaining("event-outboxer.redis.uri");
                        });
    }

    @Test
    @DisplayName("a user EntityLocker displacing the Redis locker skips connection resolution")
    void displacedLockerSkipsResolution() {
        EntityLocker custom = (key, ttl) -> Optional.empty();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisLockAutoConfiguration.class))
                .withBean("customLocker", EntityLocker.class, () -> custom)
                .withPropertyValues("event-outboxer.lock.type=redis")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(EntityLocker.class)).isSameAs(custom);
                        });
    }

    @Test
    @DisplayName(
            "a user connection bean makes event-outboxer.redis.* inert — no starter connection")
    void userBeanBeatsProperties() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisConnectionAutoConfiguration.class))
                .withUserConfiguration(SingleConnectionConfiguration.class)
                .withPropertyValues("event-outboxer.redis.host=redis.example.com")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).hasSingleBean(StatefulRedisConnection.class);
                            assertThat(ctx).doesNotHaveBean(OutboxLettuceConnectionManager.class);
                        });
    }

    @Test
    @DisplayName("without Lettuce on the classpath the connection autoconfiguration backs off")
    void backsOffWithoutLettuce() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisConnectionAutoConfiguration.class))
                .withClassLoader(new FilteredClassLoader(RedisClient.class))
                .withPropertyValues("event-outboxer.redis.host=redis.example.com")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean("outboxRedisConnection");
                        });
    }

    @Test
    @DisplayName("without redis properties the connection autoconfiguration backs off")
    void backsOffWithoutProperties() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisConnectionAutoConfiguration.class))
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx).doesNotHaveBean(StatefulRedisConnection.class);
                        });
    }

    // -------------------------------------------------------------------------------------------
    // failure analyzer
    // -------------------------------------------------------------------------------------------

    @Test
    @DisplayName("failure analyzer renders candidates and the fix for the ambiguity error")
    void analyzerRendersAmbiguityDiagnosis() {
        Throwable failure =
                new IllegalStateException(
                        "wrapped",
                        AmbiguousOutboxRedisConnectionException.noneQualified(
                                List.of("redisA", "redisB")));
        FailureAnalysis analysis = new OutboxRedisConnectionFailureAnalyzer().analyze(failure);
        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).contains("redisA").contains("redisB");
        assertThat(analysis.getAction())
                .contains("OutboxRedisConnection")
                .contains("event-outboxer.redis.uri");
    }

    @Test
    @DisplayName("failure analyzer renders the none-available diagnosis")
    void analyzerRendersNoneAvailableDiagnosis() {
        FailureAnalysis analysis =
                new OutboxRedisConnectionFailureAnalyzer()
                        .analyze(AmbiguousOutboxRedisConnectionException.noneAvailable());
        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).contains("event-outboxer.redis.uri");
    }

    @Test
    @DisplayName("failure analyzer ignores unrelated failures")
    void analyzerIgnoresUnrelatedFailures() {
        assertThat(
                        new OutboxRedisConnectionFailureAnalyzer()
                                .analyze(new IllegalStateException("boom")))
                .isNull();
    }

    // -------------------------------------------------------------------------------------------
    // fixtures
    // -------------------------------------------------------------------------------------------

    static final class ResolvedHolder {
        final StatefulRedisConnection<String, String> connection;

        ResolvedHolder(StatefulRedisConnection<String, String> connection) {
            this.connection = connection;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class StrictCaptureConfiguration {

        @Bean
        ResolvedHolder resolvedOutboxRedisConnection(
                @OutboxRedisConnection
                        ObjectProvider<StatefulRedisConnection<String, String>> qualified,
                ObjectProvider<StatefulRedisConnection<String, String>> all,
                ListableBeanFactory beanFactory) {
            return new ResolvedHolder(
                    OutboxRedisConnectionResolver.resolve(qualified, all, beanFactory));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoPlainConnectionsConfiguration {

        @Bean
        StatefulRedisConnection<String, String> redisA() {
            return stubConnection();
        }

        @Bean
        StatefulRedisConnection<String, String> redisB() {
            return stubConnection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class QualifiedPlusPlainConfiguration {

        @Bean
        @OutboxRedisConnection
        StatefulRedisConnection<String, String> outboxRedis() {
            return stubConnection();
        }

        @Bean
        StatefulRedisConnection<String, String> otherRedis() {
            return stubConnection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class QualifiedVsPrimaryConfiguration {

        @Bean
        @OutboxRedisConnection
        StatefulRedisConnection<String, String> outboxRedis() {
            return stubConnection();
        }

        @Bean
        @Primary
        StatefulRedisConnection<String, String> primaryRedis() {
            return stubConnection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrimaryPlusPlainConfiguration {

        @Bean
        @Primary
        StatefulRedisConnection<String, String> primaryRedis() {
            return stubConnection();
        }

        @Bean
        StatefulRedisConnection<String, String> otherRedis() {
            return stubConnection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SingleConnectionConfiguration {

        @Bean
        StatefulRedisConnection<String, String> onlyRedis() {
            return stubConnection();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoQualifiedConfiguration {

        @Bean
        @OutboxRedisConnection
        StatefulRedisConnection<String, String> qualifiedA() {
            return stubConnection();
        }

        @Bean
        @OutboxRedisConnection
        StatefulRedisConnection<String, String> qualifiedB() {
            return stubConnection();
        }
    }

    /**
     * Recursive dynamic proxy standing in for a Lettuce stack: every interface-returning method
     * (e.g. {@code sync()}) returns a further stub, primitives return defaults. Nothing here ever
     * reaches a real Redis.
     */
    @SuppressWarnings("unchecked")
    private static StatefulRedisConnection<String, String> stubConnection() {
        return (StatefulRedisConnection<String, String>) stub(StatefulRedisConnection.class);
    }

    private static Object stub(Class<?> type) {
        return Proxy.newProxyInstance(
                OutboxRedisConnectionSelectionTest.class.getClassLoader(),
                new Class<?>[] {type},
                (proxy, method, args) -> defaultValue(method));
    }

    private static @Nullable Object defaultValue(Method method) {
        Class<?> rt = method.getReturnType();
        if (rt == boolean.class) {
            return false;
        }
        if (rt == long.class) {
            return 0L;
        }
        if (rt == double.class) {
            return 0d;
        }
        if (rt == float.class) {
            return 0f;
        }
        if (rt == short.class) {
            return (short) 0;
        }
        if (rt == byte.class) {
            return (byte) 0;
        }
        if (rt == char.class) {
            return (char) 0;
        }
        if (rt == int.class) {
            return 0;
        }
        if (rt.isInterface()) {
            return stub(rt);
        }
        return null;
    }
}
