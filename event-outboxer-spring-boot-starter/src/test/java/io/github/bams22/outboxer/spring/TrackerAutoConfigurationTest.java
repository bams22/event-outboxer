/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.bams22.outboxer.api.track.AwaitResult;
import io.github.bams22.outboxer.api.track.OutboxEventTracker;
import io.github.bams22.outboxer.api.track.TrackedState;
import io.github.bams22.outboxer.domain.exception.AwaitInTransactionException;
import io.github.bams22.outboxer.domain.exception.InvariantViolationException;
import io.github.bams22.outboxer.spring.lock.NoOpLockAutoConfiguration;
import io.github.bams22.outboxer.spring.serializer.JacksonSerializerAutoConfiguration;
import io.github.bams22.outboxer.spring.storage.OutboxInMemoryTestConfiguration;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The {@code OutboxEventTracker} bean (ADR-0038): present in both roles, replaceable, and fed by
 * {@code event-outboxer.tracker.poll-interval}.
 */
class TrackerAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    JacksonSerializerAutoConfiguration.class,
                                    NoOpLockAutoConfiguration.class,
                                    OutboxEngineAutoConfiguration.class))
                    .withUserConfiguration(OutboxInMemoryTestConfiguration.class)
                    .withPropertyValues(
                            "event-outboxer.publish-only=true",
                            "event-outboxer.publisher.no-transaction-policy=IGNORE");

    @Test
    @DisplayName("publish-only instance → tracker bean present, defaults to a 200 ms interval")
    void presentOnPublishOnlyInstance() {
        runner.run(
                ctx -> {
                    assertThat(ctx).hasSingleBean(OutboxEventTracker.class);
                    assertThat(ctx.getBean(OutboxProperties.class).getTracker().getPollInterval())
                            .isEqualTo(Duration.ofMillis(200));
                    OutboxEventTracker tracker = ctx.getBean(OutboxEventTracker.class);
                    assertThat(tracker.state(UUID.randomUUID())).isEqualTo(TrackedState.ABSENT);
                });
    }

    @Test
    @DisplayName("event-outboxer.tracker.poll-interval binds")
    void pollIntervalBinds() {
        runner.withPropertyValues("event-outboxer.tracker.poll-interval=25ms")
                .run(
                        ctx ->
                                assertThat(
                                                ctx.getBean(OutboxProperties.class)
                                                        .getTracker()
                                                        .getPollInterval())
                                        .isEqualTo(Duration.ofMillis(25)));
    }

    @Test
    @DisplayName("non-positive poll-interval → startup fails")
    void rejectsNonPositivePollInterval() {
        runner.withPropertyValues("event-outboxer.tracker.poll-interval=0ms")
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(ctx.getStartupFailure())
                                    .hasRootCauseInstanceOf(InvariantViolationException.class)
                                    .rootCause()
                                    .hasMessageContaining("event-outboxer.tracker.poll-interval");
                        });
    }

    @Test
    @DisplayName("a user-defined OutboxEventTracker bean wins")
    void userBeanWins() {
        runner.withUserConfiguration(CustomTracker.class)
                .run(
                        ctx -> {
                            assertThat(ctx).hasSingleBean(OutboxEventTracker.class);
                            assertThat(ctx.getBean(OutboxEventTracker.class))
                                    .isSameAs(CustomTracker.INSTANCE);
                        });
    }

    @Test
    @DisplayName("outside a transaction the Spring-backed guard stays quiet")
    void awaitOutsideTransactionDoesNotThrow() {
        runner.run(
                ctx -> {
                    UUID id = UUID.randomUUID();
                    assertThat(
                                    ctx.getBean(OutboxEventTracker.class)
                                            .await(id, Duration.ofSeconds(1)))
                            .isEqualTo(new AwaitResult.Completed(id, null));
                });
    }

    @Test
    @DisplayName(
            "afterCommit still counts as inside the transaction; after the method returns it does"
                    + " not")
    void afterCommitSynchronizationIsInsideTheTransaction() {
        runner.run(
                ctx -> {
                    OutboxEventTracker tracker = ctx.getBean(OutboxEventTracker.class);
                    UUID id = UUID.randomUUID();
                    AtomicReference<Throwable> inAfterCommit = new AtomicReference<>();
                    TransactionSynchronization awaitAfterCommit =
                            new TransactionSynchronization() {
                                @Override
                                public void afterCommit() {
                                    inAfterCommit.set(
                                            catchThrowable(
                                                    () ->
                                                            tracker.await(
                                                                    id, Duration.ofSeconds(1))));
                                }
                            };

                    new TransactionTemplate(new NoOpTransactionManager())
                            .executeWithoutResult(
                                    status ->
                                            TransactionSynchronizationManager
                                                    .registerSynchronization(awaitAfterCommit));

                    assertThat(inAfterCommit.get()).isInstanceOf(AwaitInTransactionException.class);
                    assertThat(tracker.await(id, Duration.ofSeconds(1)))
                            .isEqualTo(new AwaitResult.Completed(id, null));
                });
    }

    /**
     * Drives Spring's real commit sequence — synchronisation callbacks before cleanup — without a
     * resource behind it.
     */
    static class NoOpTransactionManager extends AbstractPlatformTransactionManager {

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {}

        @Override
        protected void doCommit(DefaultTransactionStatus status) {}

        @Override
        protected void doRollback(DefaultTransactionStatus status) {}
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomTracker {

        static final OutboxEventTracker INSTANCE =
                new OutboxEventTracker() {
                    @Override
                    public TrackedState state(UUID eventId) {
                        return TrackedState.ABSENT;
                    }

                    @Override
                    public AwaitResult await(UUID eventId, Duration timeout) {
                        return new AwaitResult.Completed(eventId, null);
                    }

                    @Override
                    public AwaitResult await(
                            UUID eventId, Duration timeout, Duration pollInterval) {
                        return new AwaitResult.Completed(eventId, null);
                    }
                };

        @Bean
        OutboxEventTracker customTracker() {
            return INSTANCE;
        }
    }
}
