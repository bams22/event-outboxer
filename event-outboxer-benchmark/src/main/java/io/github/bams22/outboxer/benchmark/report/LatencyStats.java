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

import java.time.Instant;
import java.util.Arrays;

/**
 * Nearest-rank percentiles over a sample of nanosecond durations, reported in milliseconds.
 *
 * @param count sample size
 * @param p50Ms median
 * @param p95Ms 95th percentile
 * @param p99Ms 99th percentile
 * @param maxMs maximum
 * @param meanMs arithmetic mean
 */
public record LatencyStats(
        long count, double p50Ms, double p95Ms, double p99Ms, double maxMs, double meanMs) {

    private static final double NANOS_PER_MILLI = 1_000_000.0;

    /** Statistics of {@code nanos}; the array is copied, not mutated. Empty input yields zeros. */
    public static LatencyStats of(long[] nanos) {
        if (nanos.length == 0) {
            return new LatencyStats(0, 0, 0, 0, 0, 0);
        }
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        double sum = 0;
        for (long n : sorted) {
            sum += n;
        }
        return new LatencyStats(
                sorted.length,
                ms(percentile(sorted, 0.50)),
                ms(percentile(sorted, 0.95)),
                ms(percentile(sorted, 0.99)),
                ms(sorted[sorted.length - 1]),
                ms(sum / sorted.length));
    }

    /** Statistics of the first {@code count} entries of {@code nanos}. */
    public static LatencyStats of(long[] nanos, int count) {
        return of(Arrays.copyOf(nanos, count));
    }

    /** Microseconds since the epoch, the resolution the ledger compares timestamps at. */
    public static long epochMicros(Instant instant) {
        return Math.multiplyExact(instant.getEpochSecond(), 1_000_000L) + instant.getNano() / 1_000;
    }

    private static long percentile(long[] sorted, double p) {
        int rank = (int) Math.ceil(p * sorted.length);
        int index = Math.min(sorted.length - 1, Math.max(0, rank - 1));
        return sorted[index];
    }

    private static double ms(double nanos) {
        return Math.round(nanos / NANOS_PER_MILLI * 1000.0) / 1000.0;
    }
}
