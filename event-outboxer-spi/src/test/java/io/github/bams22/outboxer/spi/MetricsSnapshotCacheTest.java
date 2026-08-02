/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MetricsSnapshotCacheTest {

    private static final OutboxMetricsSnapshot SNAPSHOT =
            OutboxMetricsSnapshot.builder()
                    .totalPending(1)
                    .totalProcessing(2)
                    .totalDisabled(3)
                    .oldestPendingRunAt(null)
                    .oldestClaimedAt(null)
                    .takenAt(Instant.parse("2026-04-22T12:00:00Z"))
                    .perType(List.of())
                    .build();

    @Nested
    class Noop {

        @Test
        void getAlwaysMisses() {
            MetricsSnapshotCache cache = MetricsSnapshotCache.noop();

            cache.put(SNAPSHOT);

            assertThat(cache.get()).isEmpty();
        }

        @Test
        void invalidateIsIdempotent() {
            MetricsSnapshotCache cache = MetricsSnapshotCache.noop();

            cache.invalidate();
            cache.invalidate();

            assertThat(cache.get()).isEmpty();
        }

        @Test
        void putRejectsNull() {
            MetricsSnapshotCache cache = MetricsSnapshotCache.noop();

            assertThatThrownBy(() -> cache.put(null)).isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class InMemory {

        private final FakeClock clock = new FakeClock(Instant.parse("2026-04-22T12:00:00Z"));

        @Test
        void missWhenEmpty() {
            MetricsSnapshotCache cache =
                    MetricsSnapshotCache.inMemory(clock, Duration.ofSeconds(30));

            assertThat(cache.get()).isEmpty();
        }

        @Test
        void hitBeforeTtl() {
            MetricsSnapshotCache cache =
                    MetricsSnapshotCache.inMemory(clock, Duration.ofSeconds(30));
            cache.put(SNAPSHOT);

            clock.advance(Duration.ofSeconds(29));

            assertThat(cache.get()).contains(SNAPSHOT);
        }

        @Test
        void missAfterTtlExpiry() {
            MetricsSnapshotCache cache =
                    MetricsSnapshotCache.inMemory(clock, Duration.ofSeconds(30));
            cache.put(SNAPSHOT);

            clock.advance(Duration.ofSeconds(31));

            assertThat(cache.get()).isEmpty();
        }

        @Test
        void putReplacesExistingEntry() {
            MetricsSnapshotCache cache =
                    MetricsSnapshotCache.inMemory(clock, Duration.ofSeconds(30));
            cache.put(SNAPSHOT);

            OutboxMetricsSnapshot second =
                    OutboxMetricsSnapshot.builder()
                            .totalPending(100)
                            .totalProcessing(0)
                            .totalDisabled(0)
                            .takenAt(clock.now())
                            .perType(List.of())
                            .build();
            cache.put(second);

            assertThat(cache.get()).contains(second);
        }

        @Test
        void invalidateClearsEntry() {
            MetricsSnapshotCache cache =
                    MetricsSnapshotCache.inMemory(clock, Duration.ofSeconds(30));
            cache.put(SNAPSHOT);

            cache.invalidate();

            assertThat(cache.get()).isEmpty();
        }

        @Test
        void rejectsNonPositiveTtl() {
            assertThatThrownBy(() -> MetricsSnapshotCache.inMemory(clock, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> MetricsSnapshotCache.inMemory(clock, Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /** Minimal mutable Clock for tests — testkit.SettableClock lives in a downstream module. */
    private static final class FakeClock implements Clock {
        private final AtomicReference<Instant> now;

        FakeClock(Instant initial) {
            this.now = new AtomicReference<>(initial);
        }

        @Override
        public Instant now() {
            return now.get();
        }

        void advance(Duration d) {
            now.updateAndGet(prev -> prev.plus(d));
        }
    }
}
