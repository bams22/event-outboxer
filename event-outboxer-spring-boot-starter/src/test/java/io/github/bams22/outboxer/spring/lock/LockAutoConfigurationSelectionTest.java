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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.lock.postgres.advisory.PgAdvisoryLocker;
import io.github.bams22.outboxer.lock.postgres.lease.PgLeaseEntityLocker;
import io.github.bams22.outboxer.spi.EntityLocker;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Lock-backend selection per ADR-0022: {@code postgres-lease} means the lease locker, {@code
 * postgres-advisory} keeps the session-advisory locker — both in every relaxed-binding spelling,
 * which is the reason both gates are Binder-based conditions, not raw string matches — and a
 * user-defined {@code EntityLocker} displaces the lease locker with its probe and sweep backing off
 * silently.
 */
class LockAutoConfigurationSelectionTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    NoOpLockAutoConfiguration.class,
                                    PostgresLeaseLockAutoConfiguration.class,
                                    PostgresAdvisoryLockAutoConfiguration.class,
                                    RedisLockAutoConfiguration.class))
                    .withUserConfiguration(StubDataSourceConfiguration.class);

    @Test
    @DisplayName("lock.type=postgres-lease selects the lease locker (ADR-0022)")
    void postgresLeaseSelectsLease() {
        runner.withPropertyValues("event-outboxer.lock.type=postgres-lease")
                .run(
                        ctx -> {
                            assertThat(ctx).hasSingleBean(EntityLocker.class);
                            assertThat(ctx.getBean(EntityLocker.class))
                                    .isInstanceOf(PgLeaseEntityLocker.class);
                            assertThat(ctx).hasSingleBean(PgLeaseSweepScheduler.class);
                            assertThat(ctx).hasSingleBean(PgLeaseTableProbe.class);
                        });
    }

    @Test
    @DisplayName("relaxed-binding spellings of postgres-lease match too (Binder condition)")
    void leaseRelaxedSpellings() {
        for (String spelling : new String[] {"postgres_lease", "POSTGRES-LEASE"}) {
            runner.withPropertyValues("event-outboxer.lock.type=" + spelling)
                    .run(
                            ctx ->
                                    assertThat(ctx.getBean(EntityLocker.class))
                                            .as(
                                                    "spelling '%s' must select the lease locker",
                                                    spelling)
                                            .isInstanceOf(PgLeaseEntityLocker.class));
        }
    }

    @Test
    @DisplayName("lock.type=postgres-advisory keeps the session-advisory locker")
    void advisorySelectsAdvisory() {
        runner.withPropertyValues("event-outboxer.lock.type=postgres-advisory")
                .run(
                        ctx -> {
                            assertThat(ctx).hasSingleBean(EntityLocker.class);
                            assertThat(ctx.getBean(EntityLocker.class))
                                    .isInstanceOf(PgAdvisoryLocker.class);
                            assertThat(ctx).doesNotHaveBean(PgLeaseSweepScheduler.class);
                        });
    }

    @Test
    @DisplayName("relaxed-binding spellings of postgres-advisory match too (Binder condition)")
    void advisoryRelaxedSpellings() {
        for (String spelling : new String[] {"postgres_advisory", "POSTGRES-ADVISORY"}) {
            runner.withPropertyValues("event-outboxer.lock.type=" + spelling)
                    .run(
                            ctx ->
                                    assertThat(ctx.getBean(EntityLocker.class))
                                            .as(
                                                    "spelling '%s' must select the advisory locker",
                                                    spelling)
                                            .isInstanceOf(PgAdvisoryLocker.class));
        }
    }

    @Test
    @DisplayName("a user-defined EntityLocker displaces the lease locker; probe and sweep back off")
    void userBeanWins() {
        EntityLocker custom = (key, ttl) -> java.util.Optional.empty();
        runner.withPropertyValues("event-outboxer.lock.type=postgres-lease")
                .withBean("customLocker", EntityLocker.class, () -> custom)
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThat(ctx.getBean(EntityLocker.class)).isSameAs(custom);
                            assertThat(ctx).doesNotHaveBean(PgLeaseEntityLocker.class);
                        });
    }

    @Test
    @DisplayName("a missing entity_locks table fails startup with an actionable message")
    void probeFailsFast() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PostgresLeaseLockAutoConfiguration.class))
                .withUserConfiguration(FailingDataSourceConfiguration.class)
                .withPropertyValues("event-outboxer.lock.type=postgres-lease")
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasStackTraceContaining("V005")
                                    .hasStackTraceContaining("event-outboxer/migration/lock")
                                    .hasStackTraceContaining("postgres-advisory");
                        });
    }

    @Configuration(proxyBeanMethods = false)
    static class StubDataSourceConfiguration {

        @Bean
        DataSource dataSource() {
            return stubDataSource(false);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FailingDataSourceConfiguration {

        @Bean
        DataSource dataSource() {
            return stubDataSource(true);
        }
    }

    /**
     * Recursive dynamic proxy standing in for a JDBC stack: every interface-returning method
     * returns a further stub, primitives return defaults. With {@code failOnPrepare} the stub
     * raises {@code SQLException} from {@code prepareStatement} — the "table does not exist" shape
     * the probe must convert into an actionable startup error.
     */
    private static DataSource stubDataSource(boolean failOnPrepare) {
        return (DataSource) stub(DataSource.class, failOnPrepare);
    }

    private static Object stub(Class<?> type, boolean failOnPrepare) {
        InvocationHandler handler =
                (proxy, method, args) -> {
                    if (failOnPrepare && method.getName().equals("prepareStatement")) {
                        throw new SQLException(
                                "relation \"event_outboxer.entity_locks\" does not exist");
                    }
                    return defaultValue(method, failOnPrepare);
                };
        return Proxy.newProxyInstance(
                LockAutoConfigurationSelectionTest.class.getClassLoader(),
                new Class<?>[] {type},
                handler);
    }

    private static Object defaultValue(Method method, boolean failOnPrepare) {
        Class<?> rt = method.getReturnType();
        if (rt == void.class) {
            return null;
        }
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
        if (rt.isPrimitive()) {
            return 0;
        }
        if (rt.isInterface()) {
            return stub(rt, failOnPrepare);
        }
        return null;
    }
}
