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
import org.junit.jupiter.api.Test;

/**
 * The {@code smoke} preset with the Redis locker on a disposable Redis: exclusivity graded, the
 * lease table absent, the lock traffic visible on the Redis side and not on the PostgreSQL side.
 */
class BenchmarkRedisLockIT {

    @Test
    void redisLockerSerializesKeysAndLeavesNothingBehind() throws Exception {
        BenchmarkOptions options =
                BenchmarkOptions.parse(
                        "--bench.scenario=smoke",
                        "--bench.lock=redis",
                        "--bench.report-dir=target/bench-it");

        BenchmarkReport report = new BenchmarkRun(options, new OutboxerTarget()).run();
        new ReportWriter().printSummary(report, System.out);

        assertThat(report.passed()).isTrue();
        assertThat(report.invariants().lockExclusivityExpected()).isTrue();
        assertThat(report.invariants().lockOverlaps()).isZero();
        assertThat(report.redis()).isNotNull();
        // SET NX PX + EVAL per handled event, plus heartbeat-free background noise.
        assertThat(report.redis().commands())
                .isGreaterThanOrEqualTo(2L * options.scenario().events());
        assertThat(report.redis().remainingLockKeys()).isZero();
        assertThat(report.environment().redisVersion()).isNotBlank();
    }
}
