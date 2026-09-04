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

import io.github.bams22.outboxer.benchmark.report.BenchmarkReport;
import io.github.bams22.outboxer.benchmark.report.ReportWriter;
import io.github.bams22.outboxer.benchmark.target.BenchmarkTarget;
import io.github.bams22.outboxer.benchmark.target.outboxer.OutboxerTarget;
import java.nio.file.Path;

/**
 * Entry point of the executable jar. Exit code {@code 0} when the run passed, {@code 1} when an
 * invariant failed or the drain timed out, {@code 2} when the run could not be executed at all.
 *
 * <pre>
 * java -jar event-outboxer-benchmark-*-exec.jar --bench.scenario=throughput --bench.workers=3
 * java -jar event-outboxer-benchmark-*-exec.jar --bench.scenario=hot-key --bench.lock=noop
 * java -jar event-outboxer-benchmark-*-exec.jar --bench.scenario=backlog \
 *      --bench.jdbc-url=jdbc:postgresql://db:5432/bench --bench.jdbc-user=bench --bench.jdbc-password=...
 * </pre>
 */
public final class BenchmarkRunner {

    private BenchmarkRunner() {}

    public static void main(String[] args) {
        int code;
        try {
            BenchmarkOptions options = BenchmarkOptions.parse(args);
            BenchmarkTarget target = target(options.targetName());
            BenchmarkReport report = new BenchmarkRun(options, target).run();
            ReportWriter writer = new ReportWriter();
            Path file = writer.writeJson(report, options.reportDir());
            writer.printSummary(report, System.out);
            System.out.println("report       " + file.toAbsolutePath());
            code = report.passed() ? 0 : 1;
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            code = 2;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            code = 2;
        }
        System.exit(code);
    }

    private static BenchmarkTarget target(String name) {
        if ("outboxer".equalsIgnoreCase(name) || "event-outboxer".equalsIgnoreCase(name)) {
            return new OutboxerTarget();
        }
        throw new IllegalArgumentException(
                "Unknown target '"
                        + name
                        + "'. This repository ships only 'outboxer'; other targets are adapters"
                        + " kept outside it (ADR-0034 §2).");
    }
}
