/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.report;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.benchmark.ledger.Handling;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConcurrencyStatsTest {

    private static final Instant T0 = Instant.parse("2026-09-04T10:00:00Z");

    @Test
    void peakCountsOverlappingIntervalsPerWorkerAndFleetWide() {
        List<Handling> handlings =
                List.of(
                        handling(1, "w1", "t1", 0, 10),
                        handling(2, "w1", "t2", 5, 15),
                        handling(3, "w1", "t3", 8, 9),
                        handling(4, "w2", "t1", 0, 20),
                        handling(5, "w2", "t1", 30, 40));

        ConcurrencyStats stats = ConcurrencyStats.of(handlings, 42);

        assertThat(stats.peakPerWorker()).isEqualTo(3);
        assertThat(stats.peakInFlight()).isEqualTo(4);
        assertThat(stats.distinctThreads()).isEqualTo(4);
        assertThat(stats.peakPlatformThreads()).isEqualTo(42);
    }

    @Test
    void touchingIntervalsDoNotOverlap() {
        List<Handling> handlings =
                List.of(handling(1, "w1", "t1", 0, 10), handling(2, "w1", "t1", 10, 20));

        ConcurrencyStats stats = ConcurrencyStats.of(handlings, null);

        assertThat(stats.peakInFlight()).isEqualTo(1);
        assertThat(stats.peakPlatformThreads()).isNull();
    }

    @Test
    void emptyLedgerIsZero() {
        ConcurrencyStats stats = ConcurrencyStats.of(List.of(), null);

        assertThat(stats.peakInFlight()).isZero();
        assertThat(stats.peakPerWorker()).isZero();
        assertThat(stats.distinctThreads()).isZero();
    }

    private static Handling handling(
            long seq, String worker, String thread, long startMs, long endMs) {
        return Handling.builder()
                .seq(seq)
                .eventType("bench")
                .attempt(1)
                .workerId(worker)
                .thread(thread)
                .startedAt(T0.plusMillis(startMs))
                .finishedAt(T0.plusMillis(endMs))
                .outcome(Handling.Outcome.SUCCESS)
                .build();
    }
}
