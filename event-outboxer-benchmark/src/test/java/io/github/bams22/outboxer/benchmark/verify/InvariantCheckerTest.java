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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.benchmark.ledger.Handling;
import java.time.Instant;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

class InvariantCheckerTest {

    private static final Instant T0 = Instant.parse("2026-09-04T10:00:00Z");

    private final InvariantChecker checker = new InvariantChecker();

    @Test
    void everyEventHandledOncePasses() {
        List<Handling> ledger = List.of(ok(0, 0, 10), ok(1, 20, 30), ok(2, 40, 50));

        InvariantReport report = checker.check(3, ledger, true);

        assertThat(report.passed()).isTrue();
        assertThat(report.succeeded()).isEqualTo(3);
        assertThat(report.lost()).isZero();
        assertThat(report.duplicatedEvents()).isZero();
        assertThat(report.retries()).isZero();
        assertThat(report.lockOverlaps()).isZero();
    }

    @Test
    void missingEventIsLost() {
        InvariantReport report = checker.check(3, List.of(ok(0, 0, 10), ok(2, 40, 50)), true);

        assertThat(report.passed()).isFalse();
        assertThat(report.lost()).isEqualTo(1);
        assertThat(report.lostSample()).containsExactly(1L);
    }

    @Test
    void twoSuccessesForOneEventAreADuplicate() {
        List<Handling> ledger = List.of(ok(0, 0, 10), ok(0, 20, 30), ok(1, 40, 50));

        InvariantReport report = checker.check(2, ledger, true);

        assertThat(report.passed()).isFalse();
        assertThat(report.duplicatedEvents()).isEqualTo(1);
        assertThat(report.extraHandlings()).isEqualTo(1);
        assertThat(report.duplicateSample()).containsExactly(0L);
    }

    @Test
    void retryFollowedBySuccessIsNotADuplicate() {
        List<Handling> ledger =
                List.of(
                        handling(0, 1, Handling.Outcome.RETRY, null, 0, 5),
                        handling(0, 2, Handling.Outcome.SUCCESS, null, 100, 110));

        InvariantReport report = checker.check(1, ledger, true);

        assertThat(report.passed()).isTrue();
        assertThat(report.retries()).isEqualTo(1);
        assertThat(report.failedAttempts()).isEqualTo(1);
        assertThat(report.duplicatedEvents()).isZero();
    }

    @Test
    void overlapOnSameLockKeyFailsOnlyWhenExclusivityExpected() {
        List<Handling> ledger =
                List.of(
                        handling(0, 1, Handling.Outcome.SUCCESS, "k", 0, 100),
                        handling(1, 1, Handling.Outcome.SUCCESS, "k", 50, 150),
                        handling(2, 1, Handling.Outcome.SUCCESS, "other", 60, 70));

        InvariantReport graded = checker.check(3, ledger, true);
        InvariantReport informational = checker.check(3, ledger, false);

        assertThat(graded.lockOverlaps()).isEqualTo(1);
        assertThat(graded.passed()).isFalse();
        assertThat(graded.overlapSample()).singleElement().asString().startsWith("k: seq 0");
        assertThat(informational.lockOverlaps()).isEqualTo(1);
        assertThat(informational.passed()).isTrue();
    }

    @Test
    void longHandlingIsRememberedAcrossShorterSuccessors() {
        // seq 0 runs 0..200; seq 1 (10..20) overlaps it, and so does seq 2 (30..40) even though
        // seq 1 finished before seq 2 started — the sweep must keep the latest finish, not the
        // previous entry.
        List<Handling> ledger =
                List.of(
                        handling(0, 1, Handling.Outcome.SUCCESS, "k", 0, 200),
                        handling(1, 1, Handling.Outcome.SUCCESS, "k", 10, 20),
                        handling(2, 1, Handling.Outcome.SUCCESS, "k", 30, 40));

        assertThat(checker.check(3, ledger, true).lockOverlaps()).isEqualTo(2);
    }

    @Test
    void backToBackHandlingsDoNotOverlap() {
        List<Handling> ledger =
                List.of(
                        handling(0, 1, Handling.Outcome.SUCCESS, "k", 0, 100),
                        handling(1, 1, Handling.Outcome.SUCCESS, "k", 100, 200));

        assertThat(checker.check(2, ledger, true).lockOverlaps()).isZero();
    }

    @Test
    void handlingOfUnpublishedSeqFails() {
        InvariantReport report = checker.check(1, List.of(ok(0, 0, 1), ok(7, 2, 3)), true);

        assertThat(report.unexpected()).isEqualTo(1);
        assertThat(report.passed()).isFalse();
    }

    private static Handling ok(long seq, long startMs, long endMs) {
        return handling(seq, 1, Handling.Outcome.SUCCESS, null, startMs, endMs);
    }

    private static Handling handling(
            long seq,
            int attempt,
            Handling.Outcome outcome,
            @Nullable String lockKey,
            long startMs,
            long endMs) {
        return Handling.builder()
                .seq(seq)
                .eventType("BENCH_0")
                .attempt(attempt)
                .workerId("w0")
                .thread("t")
                .lockKey(lockKey)
                .startedAt(T0.plusMillis(startMs))
                .finishedAt(T0.plusMillis(endMs))
                .outcome(outcome)
                .build();
    }
}
