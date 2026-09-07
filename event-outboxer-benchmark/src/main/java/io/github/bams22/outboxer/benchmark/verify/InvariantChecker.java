/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.verify;

import io.github.bams22.outboxer.benchmark.ledger.Handling;
import io.github.bams22.outboxer.benchmark.report.LatencyStats;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.jspecify.annotations.Nullable;

/**
 * Grades a ledger against the published set. Stateless; every rule is a pass over the handlings.
 *
 * <p>Overlap detection sorts each lock key's handlings by start time and sweeps with the latest
 * finish seen so far: a handling that starts before that finish overlapped a predecessor. Valid on
 * one host (the in-process fleet), where every timestamp comes from the same clock; the forked
 * fleet across machines will need a tolerance for clock skew.
 */
public final class InvariantChecker {

    /** How many offenders each sample list keeps. */
    public static final int SAMPLE_SIZE = 20;

    /** Grades {@code handlings} for a run without chaos and without dedup keys. */
    public InvariantReport check(
            long published, List<Handling> handlings, boolean lockExclusivityExpected) {
        return check(published, handlings, lockExclusivityExpected, List.of(), null);
    }

    /** Grades {@code handlings} for a run without dedup keys. */
    public InvariantReport check(
            long published,
            List<Handling> handlings,
            boolean lockExclusivityExpected,
            List<ChaosEvent> chaos) {
        return check(published, handlings, lockExclusivityExpected, chaos, null);
    }

    /**
     * Grades {@code handlings} for a run that published sequence numbers {@code 0..published-1}.
     *
     * @param lockExclusivityExpected {@code true} when the scenario ran a real locker, so overlaps
     *     fail the run; {@code false} reports them as information (the hot-key baseline)
     * @param chaos what the harness did on purpose; a duplicated event whose successful handling
     *     falls into one of these windows is attributable and does not fail the run. A genuine
     *     duplicate that happens to land inside a window is masked — the price of the rule.
     * @param dedup what a run with dedup keys promises per key, {@code null} when keys were off.
     *     With it, the per-sequence "lost" rule is replaced by the per-key freshness rule: every
     *     key's last publish must be followed by a successful handling of that key that started
     *     after the publish committed.
     */
    public InvariantReport check(
            long published,
            List<Handling> handlings,
            boolean lockExclusivityExpected,
            List<ChaosEvent> chaos,
            @Nullable DedupExpectation dedup) {
        Objects.requireNonNull(handlings, "handlings must not be null");
        Objects.requireNonNull(chaos, "chaos must not be null");
        if (published < 0) {
            throw new IllegalArgumentException("published must be >= 0, got " + published);
        }

        Map<Long, Integer> successesPerSeq = new HashMap<>();
        Map<Long, List<Handling>> successes = new HashMap<>();
        long unexpected = 0;
        long retries = 0;
        long failedAttempts = 0;
        for (Handling h : handlings) {
            if (h.seq() < 0 || h.seq() >= published) {
                unexpected++;
            }
            if (h.attempt() > 1) {
                retries++;
            }
            if (h.succeeded()) {
                successesPerSeq.merge(h.seq(), 1, Integer::sum);
                successes.computeIfAbsent(h.seq(), k -> new ArrayList<>()).add(h);
            } else {
                failedAttempts++;
            }
        }

        List<Long> lostSample = new ArrayList<>();
        long lost = 0;
        if (dedup == null) {
            for (long seq = 0; seq < published; seq++) {
                if (!successesPerSeq.containsKey(seq)) {
                    lost++;
                    if (lostSample.size() < SAMPLE_SIZE) {
                        lostSample.add(seq);
                    }
                }
            }
        }
        Freshness freshness = dedup == null ? Freshness.NONE : checkFreshness(dedup, handlings);

        long duplicatedEvents = 0;
        long attributable = 0;
        long extraHandlings = 0;
        List<Long> duplicateSample = new ArrayList<>();
        for (Map.Entry<Long, Integer> e : new TreeMap<>(successesPerSeq).entrySet()) {
            if (e.getValue() > 1) {
                duplicatedEvents++;
                extraHandlings += e.getValue() - 1;
                if (explained(successes.get(e.getKey()), chaos)) {
                    attributable++;
                } else if (duplicateSample.size() < SAMPLE_SIZE) {
                    duplicateSample.add(e.getKey());
                }
            }
        }

        Overlaps overlaps = findOverlaps(handlings);

        return InvariantReport.builder()
                .published(published)
                .succeeded(successesPerSeq.size())
                .lost(lost)
                .lostSample(lostSample)
                .duplicatedEvents(duplicatedEvents)
                .attributableDuplicates(attributable)
                .unexplainedDuplicates(duplicatedEvents - attributable)
                .extraHandlings(extraHandlings)
                .duplicateSample(duplicateSample)
                .unexpected(unexpected)
                .retries(retries)
                .failedAttempts(failedAttempts)
                .lockExclusivityExpected(lockExclusivityExpected)
                .lockOverlaps(overlaps.count)
                .overlapSample(overlaps.sample)
                .dedupKeys(dedup == null ? 0 : dedup.lastCommitMicrosByKey().size())
                .coalescedPublishes(dedup == null ? 0 : published - successesPerSeq.size())
                .staleKeys(freshness.stale)
                .staleKeySample(freshness.sample)
                .build();
    }

    /**
     * Per key, the latest start of a successful handling must not precede the driver-side commit
     * instant of the key's last publish. Timestamps compare at microsecond resolution and come from
     * the same host for the in-process fleet.
     */
    private static Freshness checkFreshness(DedupExpectation dedup, List<Handling> handlings) {
        Map<String, Long> latestStart = new HashMap<>();
        for (Handling h : handlings) {
            String key = h.dedupKey();
            if (key == null || !h.succeeded()) {
                continue;
            }
            latestStart.merge(key, LatencyStats.epochMicros(h.startedAt()), Math::max);
        }
        long stale = 0;
        List<String> sample = new ArrayList<>();
        for (Map.Entry<String, Long> e : new TreeMap<>(dedup.lastCommitMicrosByKey()).entrySet()) {
            Long start = latestStart.get(e.getKey());
            if (start == null || start < e.getValue()) {
                stale++;
                if (sample.size() < SAMPLE_SIZE) {
                    sample.add(
                            e.getKey()
                                    + ": last publish committed at "
                                    + e.getValue()
                                    + "us, latest successful start "
                                    + (start == null ? "none" : start + "us"));
                }
            }
        }
        return new Freshness(stale, sample);
    }

    private record Freshness(long stale, List<String> sample) {
        static final Freshness NONE = new Freshness(0, List.of());
    }

    private static boolean explained(List<Handling> successes, List<ChaosEvent> chaos) {
        for (Handling h : successes) {
            for (ChaosEvent event : chaos) {
                if (event.explains(h.workerId(), h.finishedAt())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Overlaps findOverlaps(List<Handling> handlings) {
        Map<String, List<Handling>> byKey = new HashMap<>();
        for (Handling h : handlings) {
            String key = h.lockKey();
            if (key != null) {
                byKey.computeIfAbsent(key, k -> new ArrayList<>()).add(h);
            }
        }
        long count = 0;
        List<String> sample = new ArrayList<>();
        Comparator<Handling> byStart =
                Comparator.comparing(Handling::startedAt).thenComparing(Handling::finishedAt);
        for (Map.Entry<String, List<Handling>> e : new TreeMap<>(byKey).entrySet()) {
            List<Handling> sorted = new ArrayList<>(e.getValue());
            sorted.sort(byStart);
            @Nullable Handling latest = null;
            for (Handling h : sorted) {
                if (latest != null && h.startedAt().isBefore(latest.finishedAt())) {
                    count++;
                    if (sample.size() < SAMPLE_SIZE) {
                        sample.add(describe(e.getKey(), latest, h));
                    }
                }
                if (latest == null || h.finishedAt().isAfter(latest.finishedAt())) {
                    latest = h;
                }
            }
        }
        return new Overlaps(count, sample);
    }

    private static String describe(String key, Handling earlier, Handling later) {
        Instant overlapStart = later.startedAt();
        Instant overlapEnd = earlier.finishedAt();
        return key
                + ": seq "
                + earlier.seq()
                + " ("
                + earlier.workerId()
                + ", attempt "
                + earlier.attempt()
                + ") still running when seq "
                + later.seq()
                + " ("
                + later.workerId()
                + ", attempt "
                + later.attempt()
                + ") started; overlap "
                + overlapStart
                + " .. "
                + overlapEnd;
    }

    private record Overlaps(long count, List<String> sample) {}
}
