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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class ChaosAttributionTest {

    private static final Instant T0 = Instant.parse("2026-09-04T12:00:00Z");
    private static final Instant KILL = T0.plusSeconds(60);

    private final InvariantChecker checker = new InvariantChecker();

    @Test
    void duplicateByKilledWorkerJustBeforeTheKillIsAttributable() {
        List<Handling> ledger =
                List.of(
                        ok(0, "w0", KILL.minusMillis(300)),
                        ok(0, "w1", KILL.plusSeconds(8)),
                        ok(1, "w1", KILL.plusSeconds(9)));
        ChaosEvent kill = new ChaosEvent(ChaosEvent.Kind.WORKER_KILLED, KILL, 1, List.of("w0"), "");

        InvariantReport report = checker.check(2, ledger, true, List.of(kill));

        assertThat(report.duplicatedEvents()).isEqualTo(1);
        assertThat(report.attributableDuplicates()).isEqualTo(1);
        assertThat(report.unexplainedDuplicates()).isZero();
        assertThat(report.duplicateSample()).isEmpty();
        assertThat(report.passed()).isTrue();
    }

    @Test
    void duplicateOnSurvivingWorkersIsNotExplainedByAKill() {
        List<Handling> ledger =
                List.of(ok(0, "w1", KILL.minusMillis(300)), ok(0, "w2", KILL.plusSeconds(2)));
        ChaosEvent kill = new ChaosEvent(ChaosEvent.Kind.WORKER_KILLED, KILL, 1, List.of("w0"), "");

        InvariantReport report = checker.check(1, ledger, true, List.of(kill));

        assertThat(report.attributableDuplicates()).isZero();
        assertThat(report.unexplainedDuplicates()).isEqualTo(1);
        assertThat(report.duplicateSample()).containsExactly(0L);
        assertThat(report.passed()).isFalse();
    }

    @Test
    void duplicateByKilledWorkerLongBeforeTheKillIsNotExplained() {
        Instant early = KILL.minus(ChaosEvent.ATTRIBUTION_WINDOW).minusSeconds(1);
        List<Handling> ledger = List.of(ok(0, "w0", early), ok(0, "w1", early.plusSeconds(1)));
        ChaosEvent kill = new ChaosEvent(ChaosEvent.Kind.WORKER_KILLED, KILL, 1, List.of("w0"), "");

        assertThat(checker.check(1, ledger, true, List.of(kill)).passed()).isFalse();
    }

    @Test
    void databaseRestartExplainsAnyWorkerInsideTheWindow() {
        Instant restart = T0.plusSeconds(30);
        List<Handling> ledger =
                List.of(ok(0, "w2", restart.minusMillis(50)), ok(0, "w1", restart.plusSeconds(5)));
        ChaosEvent event =
                new ChaosEvent(ChaosEvent.Kind.POSTGRES_RESTARTED, restart, 1, List.of(), "fast");

        InvariantReport report = checker.check(1, ledger, true, List.of(event));

        assertThat(report.attributableDuplicates()).isEqualTo(1);
        assertThat(report.passed()).isTrue();
    }

    @Test
    void spawnEventsExplainNothing() {
        ChaosEvent spawn =
                new ChaosEvent(ChaosEvent.Kind.WORKER_SPAWNED, KILL, 1, List.of("w3"), "");
        assertThat(spawn.explains("w3", KILL)).isFalse();
    }

    @Test
    void lostEventsStayLostWhateverTheChaos() {
        ChaosEvent kill = new ChaosEvent(ChaosEvent.Kind.WORKER_KILLED, KILL, 1, List.of("w0"), "");
        InvariantReport report = checker.check(2, List.of(ok(0, "w1", KILL)), true, List.of(kill));

        assertThat(report.lost()).isEqualTo(1);
        assertThat(report.passed()).isFalse();
    }

    private static Handling ok(long seq, String worker, Instant finishedAt) {
        return Handling.builder()
                .seq(seq)
                .eventType("BENCH_0")
                .attempt(1)
                .workerId(worker)
                .thread("t")
                .startedAt(finishedAt.minus(Duration.ofMillis(2)))
                .finishedAt(finishedAt)
                .outcome(Handling.Outcome.SUCCESS)
                .build();
    }
}
