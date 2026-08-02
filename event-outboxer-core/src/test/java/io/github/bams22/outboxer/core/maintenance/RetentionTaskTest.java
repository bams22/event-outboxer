/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.core.maintenance;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.core.config.RetentionConfig;
import io.github.bams22.outboxer.domain.ArchivedEvent;
import io.github.bams22.outboxer.domain.Event;
import io.github.bams22.outboxer.domain.EventStatus;
import io.github.bams22.outboxer.spi.AdminCursor;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RetentionTaskTest {

    @Test
    @DisplayName("a pass loops in batches until a short batch, per enabled dimension only")
    void sweepsUntilShortBatch() {
        // 2 full batches of 10 + a short batch of 3 in the archive; disabled retention off.
        AtomicInteger archiveRemaining = new AtomicInteger(23);
        AtomicInteger disabledCalls = new AtomicInteger();
        OutboxAdmin admin =
                new StubAdmin() {
                    @Override
                    public int purgeArchive(Instant archivedBefore, int limit) {
                        int purged = Math.min(limit, archiveRemaining.get());
                        archiveRemaining.addAndGet(-purged);
                        return purged;
                    }

                    @Override
                    public int purgeDisabled(@Nullable String type, Instant olderThan, int limit) {
                        disabledCalls.incrementAndGet();
                        return 0;
                    }
                };
        RetentionTask task =
                new RetentionTask(
                        admin,
                        Clock.system(),
                        RetentionConfig.builder()
                                .archiveOlderThan(Duration.ofDays(30))
                                .disabledOlderThan(null)
                                .batchSize(10)
                                .interval(Duration.ofHours(1))
                                .build());

        task.run();

        assertThat(archiveRemaining).hasValue(0);
        assertThat(disabledCalls).hasValue(0);
    }

    @Test
    @DisplayName("a failing sweep is contained: the other dimension still runs, nothing propagates")
    void sweepFailureIsContained() {
        AtomicInteger disabledPurged = new AtomicInteger();
        OutboxAdmin admin =
                new StubAdmin() {
                    @Override
                    public int purgeArchive(Instant archivedBefore, int limit) {
                        throw new IllegalStateException("archive table missing (simulated)");
                    }

                    @Override
                    public int purgeDisabled(@Nullable String type, Instant olderThan, int limit) {
                        disabledPurged.incrementAndGet();
                        return 0;
                    }
                };
        RetentionTask task =
                new RetentionTask(
                        admin,
                        Clock.system(),
                        RetentionConfig.builder()
                                .archiveOlderThan(Duration.ofDays(30))
                                .disabledOlderThan(Duration.ofDays(90))
                                .batchSize(10)
                                .interval(Duration.ofHours(1))
                                .build());

        task.run(); // must not throw

        assertThat(disabledPurged).hasValue(1);
    }

    @Test
    @DisplayName("RetentionConfig.disabled() reports not enabled")
    void disabledConfig() {
        assertThat(RetentionConfig.disabled().enabled()).isFalse();
        assertThat(
                        RetentionConfig.builder()
                                .archiveOlderThan(Duration.ofDays(1))
                                .disabledOlderThan(null)
                                .batchSize(10)
                                .interval(Duration.ofHours(1))
                                .build()
                                .enabled())
                .isTrue();
    }

    /** All-empty base so tests override only what they observe. */
    private static class StubAdmin implements OutboxAdmin {
        @Override
        public List<Event> findByStatus(
                EventStatus status,
                @Nullable String eventType,
                int limit,
                @Nullable AdminCursor after) {
            return new ArrayList<>();
        }

        @Override
        public Optional<ArchivedEvent> findInArchive(UUID id) {
            return Optional.empty();
        }

        @Override
        public boolean reenable(UUID id) {
            return false;
        }

        @Override
        public int reenableAll(String eventType, @Nullable Instant createdBefore, int limit) {
            return 0;
        }

        @Override
        public int purgeDisabled(@Nullable String eventType, Instant olderThan, int limit) {
            return 0;
        }

        @Override
        public int purgeArchive(Instant archivedBefore, int limit) {
            return 0;
        }
    }
}
