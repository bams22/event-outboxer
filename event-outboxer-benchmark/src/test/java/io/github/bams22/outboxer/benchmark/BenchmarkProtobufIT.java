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
import io.github.bams22.outboxer.benchmark.scenario.PayloadFormat;
import io.github.bams22.outboxer.benchmark.target.outboxer.OutboxerTarget;
import org.junit.jupiter.api.Test;

/**
 * The {@code smoke} preset with the Protobuf write format: the starter switches the writer to
 * {@code protobuf}, events land in the BYTEA lane, handlers get the generated message back with seq
 * and lock key intact (graded through the ledger like any other run).
 */
class BenchmarkProtobufIT {

    @Test
    void protobufPayloadRoundTripsThroughTheOutbox() throws Exception {
        BenchmarkOptions options =
                BenchmarkOptions.parse(
                        "--bench.scenario=smoke",
                        "--bench.payload=protobuf",
                        "--bench.report-dir=target/bench-it");

        BenchmarkReport report = new BenchmarkRun(options, new OutboxerTarget()).run();
        new ReportWriter().printSummary(report, System.out);

        assertThat(report.scenario().payloadFormat()).isEqualTo(PayloadFormat.PROTOBUF);
        assertThat(report.passed()).isTrue();
        assertThat(report.invariants().lockOverlaps()).isZero();
        assertThat(report.processing().handled()).isEqualTo(options.scenario().events());
    }
}
