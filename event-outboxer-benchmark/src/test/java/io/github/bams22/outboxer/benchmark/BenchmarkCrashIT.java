/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bams22.outboxer.benchmark.report.BenchmarkReport;
import io.github.bams22.outboxer.benchmark.report.ReportWriter;
import io.github.bams22.outboxer.benchmark.target.outboxer.OutboxerTarget;
import io.github.bams22.outboxer.benchmark.verify.ChaosEvent;
import org.junit.jupiter.api.Test;

/**
 * ADR-0034 phase 2, the kill half: a forked fleet of three, two workers {@code SIGKILL}ed mid-drain
 * and replaced. Grades {@code lost = 0}, every duplicate attributable to the kill, no lock overlap,
 * clean storage. Scaled down from the {@code crash} preset so it finishes in a minute; the recovery
 * timers are the preset's.
 */
class BenchmarkCrashIT {

    @Test
    void killedWorkersLoseNothingAndEveryDuplicateIsExplained() throws Exception {
        BenchmarkOptions options =
                BenchmarkOptions.parse(
                        "--bench.scenario=crash",
                        "--bench.events=600",
                        "--bench.handler-work-time=5ms",
                        "--bench.kill-at=0.2",
                        "--bench.report-dir=target/bench-it");

        BenchmarkReport report = new BenchmarkRun(options, new OutboxerTarget()).run();
        new ReportWriter().printSummary(report, System.out);

        assertThat(report.chaos())
                .extracting(ChaosEvent::kind)
                .containsExactly(ChaosEvent.Kind.WORKER_KILLED, ChaosEvent.Kind.WORKER_SPAWNED);
        assertThat(report.chaos().get(0).workerIds()).hasSize(2);
        assertThat(report.processing().drained()).isTrue();
        assertThat(report.invariants().lost()).isZero();
        assertThat(report.invariants().unexplainedDuplicates()).isZero();
        assertThat(report.invariants().unexpected()).isZero();
        assertThat(report.invariants().lockOverlaps()).isZero();
        assertThat(report.storage().clean()).isTrue();
        assertThat(report.passed()).isTrue();
    }
}
