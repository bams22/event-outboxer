/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.bams22.outboxer.core.publish.TransactionContext;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.MetricsSnapshotCache;
import io.github.bams22.outboxer.spi.OutboxMetricsSnapshot;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransactionAwareMetricsCacheInvalidationTest {

    private static final OutboxMetricsSnapshot SNAPSHOT =
            OutboxMetricsSnapshot.builder()
                    .totalPending(3)
                    .totalProcessing(0)
                    .totalDisabled(0)
                    .takenAt(Instant.parse("2026-04-22T12:00:00Z"))
                    .perType(List.of())
                    .build();

    @Test
    @DisplayName("with no transaction in progress the invalidation goes straight through")
    void invalidate_withoutTransaction_appliesImmediately() {
        MetricsSnapshotCache delegate =
                MetricsSnapshotCache.inMemory(Clock.system(), Duration.ofMinutes(10));
        delegate.put(SNAPSHOT);
        MetricsSnapshotCache cache =
                new TransactionAwareMetricsCacheInvalidation(delegate, new ImmediateContext());

        cache.invalidate();

        assertThat(delegate.get()).isEmpty();
    }

    @Test
    @DisplayName("inside a transaction the invalidation waits for the commit")
    void invalidate_insideTransaction_waitsForCommit() {
        MetricsSnapshotCache delegate =
                MetricsSnapshotCache.inMemory(Clock.system(), Duration.ofMinutes(10));
        delegate.put(SNAPSHOT);
        DeferringContext transaction = new DeferringContext();
        MetricsSnapshotCache cache =
                new TransactionAwareMetricsCacheInvalidation(delegate, transaction);

        cache.invalidate();

        assertThat(delegate.get())
                .as("the changed rows are not visible to other sessions yet")
                .contains(SNAPSHOT);

        transaction.commit();

        assertThat(delegate.get()).isEmpty();
    }

    @Test
    @DisplayName("a rollback drops the invalidation: nothing changed")
    void invalidate_rolledBackTransaction_neverApplies() {
        MetricsSnapshotCache delegate =
                MetricsSnapshotCache.inMemory(Clock.system(), Duration.ofMinutes(10));
        delegate.put(SNAPSHOT);
        DeferringContext transaction = new DeferringContext();
        MetricsSnapshotCache cache =
                new TransactionAwareMetricsCacheInvalidation(delegate, transaction);

        cache.invalidate();
        transaction.rollback();

        assertThat(delegate.get()).contains(SNAPSHOT);
    }

    @Test
    @DisplayName("a delegate that throws never reaches the caller")
    void invalidate_swallowsDelegateFailure() {
        MetricsSnapshotCache cache =
                new TransactionAwareMetricsCacheInvalidation(
                        new ThrowingCache(), new ImmediateContext());

        assertThatCode(cache::invalidate).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("get() and put() are plain pass-throughs")
    void readsAndWritesArePassedThrough() {
        MetricsSnapshotCache delegate =
                MetricsSnapshotCache.inMemory(Clock.system(), Duration.ofMinutes(10));
        MetricsSnapshotCache cache =
                new TransactionAwareMetricsCacheInvalidation(delegate, new ImmediateContext());

        cache.put(SNAPSHOT);

        assertThat(cache.get()).contains(SNAPSHOT);
        assertThat(delegate.get()).contains(SNAPSHOT);
    }

    /**
     * Stands in for "no Spring-managed transaction": the action runs on the spot.
     */
    private static final class ImmediateContext implements TransactionContext {
        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public void afterCommit(Runnable action) {
            action.run();
        }
    }

    /**
     * Collects after-commit actions the way Spring's synchronization manager would.
     */
    private static final class DeferringContext implements TransactionContext {
        private final List<Runnable> pending = new ArrayList<>();

        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        public void afterCommit(Runnable action) {
            pending.add(action);
        }

        void commit() {
            pending.forEach(Runnable::run);
            pending.clear();
        }

        void rollback() {
            pending.clear();
        }
    }

    private static final class ThrowingCache implements MetricsSnapshotCache {
        @Override
        public Optional<OutboxMetricsSnapshot> get() {
            return Optional.empty();
        }

        @Override
        public void put(OutboxMetricsSnapshot snapshot) {}

        @Override
        public void invalidate() {
            throw new IllegalStateException("cache backend down");
        }
    }
}
