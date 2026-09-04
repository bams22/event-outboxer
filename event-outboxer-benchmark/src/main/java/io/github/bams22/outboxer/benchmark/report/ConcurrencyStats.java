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

import io.github.bams22.outboxer.benchmark.ledger.Handling;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * How many handlers actually ran at once, measured from the ledger rather than read off the
 * configuration. A handling is in flight between its {@code startedAt} and {@code finishedAt}; the
 * peak is the largest number of handlings whose intervals overlap at one instant. With a platform
 * executor the peak is bounded by {@code handler-pool-size} per type; with the virtual-thread
 * executor by the whole in-flight budget ({@code handler-pool-size + handler-queue-capacity}).
 *
 * @param peakInFlight largest number of handlings running at once across the fleet
 * @param peakPerWorker the same, within the busiest single worker
 * @param distinctThreads distinct handler threads seen, summed over workers; equals the number of
 *     handlings for a thread-per-task executor
 * @param peakPlatformThreads {@code ThreadMXBean} peak platform thread count of the JVM that ran
 *     the fleet, driver threads included; {@code null} for a forked fleet, whose workers are other
 *     processes. Virtual threads are not counted, which is the point of reporting it
 */
public record ConcurrencyStats(
        int peakInFlight,
        int peakPerWorker,
        int distinctThreads,
        @Nullable Integer peakPlatformThreads) {

    /**
     * Sweeps the handling intervals. A handling that finishes at the very instant another starts
     * does not overlap it.
     */
    public static ConcurrencyStats of(
            List<Handling> handlings, @Nullable Integer peakPlatformThreads) {
        Objects.requireNonNull(handlings, "handlings must not be null");
        Map<String, List<Long>> perWorker = new HashMap<>();
        List<Long> all = new ArrayList<>(handlings.size() * 2);
        Set<String> threads = new HashSet<>();
        for (Handling h : handlings) {
            long start = LatencyStats.epochMicros(h.startedAt());
            long end = LatencyStats.epochMicros(h.finishedAt());
            // Ends sort before starts at the same instant: the low bit is 0 for an end.
            long startKey = (start << 1) | 1L;
            long endKey = end << 1;
            all.add(startKey);
            all.add(endKey);
            List<Long> worker = perWorker.computeIfAbsent(h.workerId(), _ -> new ArrayList<>());
            worker.add(startKey);
            worker.add(endKey);
            threads.add(h.workerId() + "/" + h.thread());
        }
        int peakPerWorker = 0;
        for (List<Long> keys : perWorker.values()) {
            peakPerWorker = Math.max(peakPerWorker, peak(keys));
        }
        return new ConcurrencyStats(peak(all), peakPerWorker, threads.size(), peakPlatformThreads);
    }

    private static int peak(List<Long> keys) {
        long[] sorted = keys.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(sorted);
        int running = 0;
        int peak = 0;
        for (long key : sorted) {
            if ((key & 1L) == 1L) {
                running++;
                peak = Math.max(peak, running);
            } else {
                running--;
            }
        }
        return peak;
    }
}
