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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import lombok.Builder;

/**
 * The verdict over one run's ledger. Counts are the truth; the samples are the first few offenders
 * so a failing run can be investigated from the report alone.
 *
 * @param published events the driver published ({@code seq} runs {@code 0..published-1})
 * @param succeeded distinct sequence numbers with at least one successful handling
 * @param lost published events with no successful handling
 * @param lostSample first offenders, at most {@link InvariantChecker#SAMPLE_SIZE}
 * @param duplicatedEvents events with more than one successful handling
 * @param attributableDuplicates of those, events one of whose successful handlings falls into a
 *     chaos event's window ({@link ChaosEvent#explains}) — the expected price of a kill or an
 *     outage under at-least-once
 * @param unexplainedDuplicates duplicated events no chaos event accounts for — a bug
 * @param extraHandlings successful handlings beyond the first, summed over all events
 * @param duplicateSample first <em>unexplained</em> duplicated sequence numbers
 * @param unexpected handlings whose sequence number was never published
 * @param retries handlings with {@code attempt > 1}
 * @param failedAttempts handlings whose outcome was not success
 * @param lockExclusivityExpected whether overlaps count against the run (a real locker was on)
 * @param lockOverlaps handlings that started while another handling of the same lock key was still
 *     running
 * @param overlapSample first overlaps, human-readable
 */
@Builder
public record InvariantReport(
        long published,
        long succeeded,
        long lost,
        List<Long> lostSample,
        long duplicatedEvents,
        long attributableDuplicates,
        long unexplainedDuplicates,
        long extraHandlings,
        List<Long> duplicateSample,
        long unexpected,
        long retries,
        long failedAttempts,
        boolean lockExclusivityExpected,
        long lockOverlaps,
        List<String> overlapSample) {

    public InvariantReport {
        lostSample = List.copyOf(Objects.requireNonNullElse(lostSample, List.of()));
        duplicateSample = List.copyOf(Objects.requireNonNullElse(duplicateSample, List.of()));
        overlapSample = List.copyOf(Objects.requireNonNullElse(overlapSample, List.of()));
    }

    /**
     * No lost event, no unexplained duplicate, nothing unexpected, and no overlap when a locker was
     * on. Duplicates a chaos event accounts for are the contract, not a defect (ADR-0015).
     */
    @JsonProperty("passed")
    public boolean passed() {
        return lost == 0
                && unexplainedDuplicates == 0
                && unexpected == 0
                && (!lockExclusivityExpected || lockOverlaps == 0);
    }
}
