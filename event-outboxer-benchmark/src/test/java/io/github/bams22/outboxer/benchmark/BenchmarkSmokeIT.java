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
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * The invariant half of ADR-0034 as a test: the {@code smoke} preset on a disposable PostgreSQL,
 * in-process fleet of two workers with the lease locker. Grades invariants only — never a number.
 */
class BenchmarkSmokeIT {

    @Test
    void smokeScenarioDrainsWithInvariantsIntact() throws Exception {
        BenchmarkOptions options =
                BenchmarkOptions.parse(
                        "--bench.scenario=smoke", "--bench.report-dir=target/bench-it");

        BenchmarkReport report = new BenchmarkRun(options, new OutboxerTarget()).run();

        ReportWriter writer = new ReportWriter();
        writer.printSummary(report, System.out);
        Path json = writer.writeJson(report, options.reportDir());

        assertThat(report.processing().drained()).isTrue();
        assertThat(report.invariants().lost()).isZero();
        assertThat(report.invariants().duplicatedEvents()).isZero();
        assertThat(report.invariants().unexpected()).isZero();
        assertThat(report.invariants().lockExclusivityExpected()).isTrue();
        assertThat(report.invariants().lockOverlaps()).isZero();
        assertThat(report.storage().clean()).isTrue();
        assertThat(report.passed()).isTrue();
        assertThat(report.processing().handled()).isEqualTo(options.scenario().events());
        assertThat(report.database().writes().total()).isPositive();
        assertThat(Files.size(json)).isPositive();
    }
}
