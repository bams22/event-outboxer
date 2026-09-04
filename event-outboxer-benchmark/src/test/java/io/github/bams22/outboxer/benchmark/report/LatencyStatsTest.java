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

import java.time.Instant;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class LatencyStatsTest {

    @Test
    void nearestRankPercentilesOverOneToHundredMillis() {
        long[] nanos = LongStream.rangeClosed(1, 100).map(ms -> ms * 1_000_000L).toArray();

        LatencyStats stats = LatencyStats.of(nanos);

        assertThat(stats.count()).isEqualTo(100);
        assertThat(stats.p50Ms()).isEqualTo(50.0);
        assertThat(stats.p95Ms()).isEqualTo(95.0);
        assertThat(stats.p99Ms()).isEqualTo(99.0);
        assertThat(stats.maxMs()).isEqualTo(100.0);
        assertThat(stats.meanMs()).isEqualTo(50.5);
    }

    @Test
    void emptySampleIsAllZeros() {
        assertThat(LatencyStats.of(new long[0])).isEqualTo(new LatencyStats(0, 0, 0, 0, 0, 0));
    }

    @Test
    void prefixOverloadIgnoresTheTail() {
        long[] nanos = {5_000_000L, 1_000_000L, 999_000_000L};

        LatencyStats stats = LatencyStats.of(nanos, 2);

        assertThat(stats.count()).isEqualTo(2);
        assertThat(stats.maxMs()).isEqualTo(5.0);
    }

    @Test
    void inputIsNotMutated() {
        long[] nanos = {3, 1, 2};
        LatencyStats.of(nanos);
        assertThat(nanos).containsExactly(3, 1, 2);
    }

    @Test
    void epochMicrosKeepsSubMillisecondResolution() {
        Instant t = Instant.parse("2026-09-04T10:00:00.123456789Z");
        assertThat(LatencyStats.epochMicros(t))
                .isEqualTo(t.getEpochSecond() * 1_000_000L + 123_456);
    }
}
