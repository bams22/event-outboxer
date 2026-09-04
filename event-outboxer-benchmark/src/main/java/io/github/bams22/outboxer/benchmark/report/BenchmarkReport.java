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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.bams22.outboxer.benchmark.db.StorageState;
import io.github.bams22.outboxer.benchmark.db.TableWrites;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import io.github.bams22.outboxer.benchmark.verify.ChaosEvent;
import io.github.bams22.outboxer.benchmark.verify.InvariantReport;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Everything one run produced, in the shape the JSON file takes. The scenario and environment
 * travel with the numbers so a report is self-describing (ADR-0034 §7).
 *
 * @param target the {@code BenchmarkTarget} name
 * @param scenario the effective scenario
 * @param environment JVM, host and database description
 * @param startedAt wall-clock start of the run
 * @param finishedAt wall-clock end of the run
 * @param publish the publish phase
 * @param processing the handling phase
 * @param database row writes attributed to the outbox schema
 * @param invariants the checker's verdict
 * @param storage what the schema looked like after the fleet stopped
 * @param chaos what the harness did on purpose, in order
 */
@Builder
public record BenchmarkReport(
        String target,
        Scenario scenario,
        Environment environment,
        Instant startedAt,
        Instant finishedAt,
        PublishMetrics publish,
        ProcessingMetrics processing,
        DatabaseMetrics database,
        InvariantReport invariants,
        StorageState storage,
        List<ChaosEvent> chaos) {

    public BenchmarkReport {
        Objects.requireNonNull(target, "target must not be null");
        Objects.requireNonNull(scenario, "scenario must not be null");
        Objects.requireNonNull(environment, "environment must not be null");
        Objects.requireNonNull(startedAt, "startedAt must not be null");
        Objects.requireNonNull(finishedAt, "finishedAt must not be null");
        Objects.requireNonNull(publish, "publish must not be null");
        Objects.requireNonNull(processing, "processing must not be null");
        Objects.requireNonNull(database, "database must not be null");
        Objects.requireNonNull(invariants, "invariants must not be null");
        Objects.requireNonNull(storage, "storage must not be null");
        chaos = List.copyOf(Objects.requireNonNullElse(chaos, List.of()));
    }

    /**
     * The run's overall verdict: drained in time, invariants hold, storage is clean. Numbers never
     * take part (ADR-0034 §5).
     */
    @JsonProperty("passed")
    public boolean passed() {
        return processing.drained() && invariants.passed() && storage.clean();
    }

    /**
     * Where and on what the run happened.
     *
     * @param javaVersion {@code java.version}
     * @param os {@code os.name os.version (os.arch)}
     * @param availableProcessors what the JVM saw
     * @param maxHeapMb {@code -Xmx} as the JVM resolved it
     * @param host host name
     * @param databaseOrigin {@code external} or {@code testcontainers:<image>}
     * @param postgresVersion {@code server_version}
     * @param libraryVersion event-outboxer version from the api jar manifest, if present
     */
    @Builder
    public record Environment(
            String javaVersion,
            String os,
            int availableProcessors,
            long maxHeapMb,
            String host,
            String databaseOrigin,
            String postgresVersion,
            String libraryVersion) {}

    /**
     * The publish phase.
     *
     * @param events events published
     * @param duration wall time from first to last publish call
     * @param perSecond {@code events / duration}
     * @param latency per-call latency including the surrounding transaction
     */
    public record PublishMetrics(
            long events, Duration duration, double perSecond, LatencyStats latency) {}

    /**
     * The handling phase.
     *
     * @param drained whether every event was handled before the drain timeout
     * @param handled distinct events with a successful handling
     * @param totalHandlings all handler invocations, retries included
     * @param duration from the phase start (publish start, or fleet start in backlog mode) to the
     *     last successful handling
     * @param perSecond {@code handled / duration}
     * @param endToEndLatency publish call start to first successful handling end, per event
     * @param retries handlings with {@code attempt > 1}
     */
    public record ProcessingMetrics(
            boolean drained,
            long handled,
            long totalHandlings,
            Duration duration,
            double perSecond,
            LatencyStats endToEndLatency,
            long retries) {}

    /**
     * Row writes in the outbox schema between the opening and closing {@code pg_stat} samples.
     *
     * @param writes the counter difference
     * @param writesPerEvent {@code writes.total() / events}
     * @param caveat why the figure is not to be trusted, {@code null} when it is. A crash restart
     *     of PostgreSQL resets the cumulative statistics, so the closing sample only covers the
     *     part of the run after it; a fast restart persists them.
     */
    public record DatabaseMetrics(
            TableWrites writes, double writesPerEvent, @Nullable String caveat) {}
}
