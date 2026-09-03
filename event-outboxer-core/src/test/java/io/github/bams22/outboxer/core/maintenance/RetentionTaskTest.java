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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bams22.outboxer.api.observer.OutboxListener;
import io.github.bams22.outboxer.api.observer.RetentionPurgedInfo;
import io.github.bams22.outboxer.core.config.RetentionConfig;
import io.github.bams22.outboxer.spi.Clock;
import io.github.bams22.outboxer.spi.OutboxAdmin;
import io.github.bams22.outboxer.spi.contracts.support.StubOutboxAdmin;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
                new StubOutboxAdmin() {
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
        List<RetentionPurgedInfo> purgedInfos = new ArrayList<>();
        OutboxListener listener =
                new OutboxListener() {
                    @Override
                    public void onRetentionPurged(RetentionPurgedInfo info) {
                        purgedInfos.add(info);
                    }
                };
        RetentionTask task =
                new RetentionTask(
                        admin,
                        listener,
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
        assertThat(purgedInfos).containsExactly(new RetentionPurgedInfo(23, 0));
    }

    @Test
    @DisplayName(
            "a failing archive sweep still runs the disabled sweep, reports partial progress, then"
                    + " propagates")
    void sweepFailurePropagatesAfterBothSweeps() {
        AtomicInteger disabledCalls = new AtomicInteger();
        OutboxAdmin admin =
                new StubOutboxAdmin() {
                    @Override
                    public int purgeArchive(Instant archivedBefore, int limit) {
                        throw new IllegalStateException("archive table missing (simulated)");
                    }

                    @Override
                    public int purgeDisabled(@Nullable String type, Instant olderThan, int limit) {
                        return disabledCalls.incrementAndGet() == 1 ? 4 : 0;
                    }
                };
        List<RetentionPurgedInfo> purgedInfos = new ArrayList<>();
        RetentionTask task =
                new RetentionTask(
                        admin,
                        new OutboxListener() {
                            @Override
                            public void onRetentionPurged(RetentionPurgedInfo info) {
                                purgedInfos.add(info);
                            }
                        },
                        Clock.system(),
                        RetentionConfig.builder()
                                .archiveOlderThan(Duration.ofDays(30))
                                .disabledOlderThan(Duration.ofDays(90))
                                .batchSize(10)
                                .interval(Duration.ofHours(1))
                                .build());

        assertThatThrownBy(task::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("archive table missing");

        assertThat(disabledCalls).hasValue(1);
        assertThat(purgedInfos).containsExactly(new RetentionPurgedInfo(0, 4));
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
}
