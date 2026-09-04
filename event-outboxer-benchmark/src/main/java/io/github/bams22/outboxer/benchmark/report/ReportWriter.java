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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import io.github.bams22.outboxer.benchmark.verify.ChaosEvent;
import io.github.bams22.outboxer.serializer.jackson.JacksonObjectMapperFactory;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;

/**
 * Writes the JSON file and prints the console summary. The JSON is the record of the run; the
 * summary is what a person reads first.
 */
public final class ReportWriter {

    private static final DateTimeFormatter FILE_STAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);

    private final ObjectMapper mapper;

    public ReportWriter() {
        this.mapper =
                JacksonObjectMapperFactory.defaults()
                        .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                        .enable(SerializationFeature.INDENT_OUTPUT);
    }

    /** Writes {@code <dir>/<scenario>-<utc stamp>.json} and returns its path. */
    public Path writeJson(BenchmarkReport report, Path dir) {
        Objects.requireNonNull(report, "report must not be null");
        Objects.requireNonNull(dir, "dir must not be null");
        try {
            Files.createDirectories(dir);
            Path file =
                    dir.resolve(
                            report.scenario().name()
                                    + "-"
                                    + FILE_STAMP.format(report.startedAt())
                                    + ".json");
            mapper.writeValue(file.toFile(), report);
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write benchmark report to " + dir, e);
        }
    }

    /** Prints the human summary: one line per phase, the verdict last. */
    public void printSummary(BenchmarkReport report, PrintStream out) {
        Scenario s = report.scenario();
        out.printf(
                Locale.ROOT,
                "%s  scenario=%s  fleet=%s events=%d workers=%d types=%d lockKeys=%d pool=%d"
                        + " batch=%d poll=%s lock=%s exec=%s finalizeBatching=%s work=%s"
                        + " failureRate=%.2f%n",
                report.target(),
                s.name(),
                s.fleet().option(),
                s.events(),
                s.workers(),
                s.eventTypes(),
                s.lockKeyCardinality(),
                s.handlerPoolSize(),
                s.claimBatchSize(),
                human(s.pollMinInterval()),
                s.lockType().property(),
                s.executorType().property(),
                s.finalizeBatching(),
                human(s.handlerWorkTime()),
                s.failureRate());
        BenchmarkReport.Environment env = report.environment();
        out.printf(
                Locale.ROOT,
                "environment  java %s  %s  cpus=%d heap=%dMB  postgres %s (%s)  library %s%n",
                env.javaVersion(),
                env.os(),
                env.availableProcessors(),
                env.maxHeapMb(),
                env.postgresVersion(),
                env.databaseOrigin(),
                env.libraryVersion());
        BenchmarkReport.PublishMetrics p = report.publish();
        out.printf(
                Locale.ROOT,
                "publish      %d in %s = %.0f/s   p50 %.1fms p95 %.1fms p99 %.1fms max %.1fms%n",
                p.events(),
                human(p.duration()),
                p.perSecond(),
                p.latency().p50Ms(),
                p.latency().p95Ms(),
                p.latency().p99Ms(),
                p.latency().maxMs());
        BenchmarkReport.ProcessingMetrics pr = report.processing();
        out.printf(
                Locale.ROOT,
                "processing   %s %d/%d in %s = %.0f/s   e2e p50 %.0fms p95 %.0fms p99 %.0fms"
                        + " max %.0fms   handlings=%d retries=%d%n",
                pr.drained() ? "drained" : "TIMED OUT",
                pr.handled(),
                p.events(),
                human(pr.duration()),
                pr.perSecond(),
                pr.endToEndLatency().p50Ms(),
                pr.endToEndLatency().p95Ms(),
                pr.endToEndLatency().p99Ms(),
                pr.endToEndLatency().maxMs(),
                pr.totalHandlings(),
                pr.retries());
        BenchmarkReport.DatabaseMetrics db = report.database();
        out.printf(
                Locale.ROOT,
                "database     %d row writes (ins %d, upd %d, del %d) = %.2f/event%n",
                db.writes().total(),
                db.writes().inserts(),
                db.writes().updates(),
                db.writes().deletes(),
                db.writesPerEvent());
        if (db.caveat() != null) {
            out.println("             " + db.caveat());
        }
        if (report.redis() != null) {
            out.printf(
                    Locale.ROOT,
                    "redis        %d commands = %.2f/event   lock keys left=%d   (%s, %s)%n",
                    report.redis().commands(),
                    report.redis().commandsPerEvent(),
                    report.redis().remainingLockKeys(),
                    env.redisVersion(),
                    env.redisOrigin());
        }
        for (ChaosEvent c : report.chaos()) {
            out.printf(
                    Locale.ROOT,
                    "chaos        %s at %s after %d handled %s %s%n",
                    c.kind(),
                    c.at(),
                    c.progress(),
                    c.workerIds(),
                    c.details());
        }
        var inv = report.invariants();
        out.printf(
                Locale.ROOT,
                "invariants   lost=%d duplicates=%d (attributable %d, unexplained %d) unexpected=%d"
                        + " lockOverlaps=%d (%s)   storage: events=%d locks=%d%n",
                inv.lost(),
                inv.duplicatedEvents(),
                inv.attributableDuplicates(),
                inv.unexplainedDuplicates(),
                inv.unexpected(),
                inv.lockOverlaps(),
                inv.lockExclusivityExpected() ? "graded" : "informational, no locker",
                report.storage().eventRows(),
                report.storage().lockRows());
        for (String line : inv.overlapSample()) {
            out.println("             overlap: " + line);
        }
        if (!inv.lostSample().isEmpty()) {
            out.println("             lost seqs: " + inv.lostSample());
        }
        if (!inv.duplicateSample().isEmpty()) {
            out.println("             unexplained duplicated seqs: " + inv.duplicateSample());
        }
        out.println(report.passed() ? "RESULT       PASS" : "RESULT       FAIL");
    }

    private static String human(Duration d) {
        long ms = d.toMillis();
        if (ms < 1_000) {
            return ms + "ms";
        }
        return String.format(Locale.ROOT, "%.1fs", ms / 1000.0);
    }
}
