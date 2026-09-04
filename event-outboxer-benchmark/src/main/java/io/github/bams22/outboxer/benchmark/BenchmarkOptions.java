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

import io.github.bams22.outboxer.benchmark.db.DatabaseCoordinates;
import io.github.bams22.outboxer.benchmark.scenario.ExecutorType;
import io.github.bams22.outboxer.benchmark.scenario.LockType;
import io.github.bams22.outboxer.benchmark.scenario.Scenario;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.convert.DurationStyle;

/**
 * Command-line options: {@code --bench.<key>=<value>} pairs. A preset is picked with {@code
 * --bench.scenario}, every scenario knob can then be overridden by name, and the run-level options
 * (database, report directory, target) sit beside them. Unknown {@code bench.*} keys are an error
 * that lists the known ones; anything not under {@code bench.} is ignored so Spring-style arguments
 * can pass through.
 *
 * @param scenario the effective scenario after overrides
 * @param targetName which {@code BenchmarkTarget} to run; only {@code outboxer} ships here
 * @param database external database, {@code null} = start a disposable PostgreSQL
 * @param postgresImage image for the disposable database
 * @param reportDir where the JSON report goes
 */
public record BenchmarkOptions(
        Scenario scenario,
        String targetName,
        @Nullable DatabaseCoordinates database,
        String postgresImage,
        Path reportDir) {

    /** Prefix every option carries. */
    public static final String PREFIX = "--bench.";

    private static final String DEFAULT_TARGET = "outboxer";
    private static final String DEFAULT_IMAGE = "postgres:15";
    private static final Path DEFAULT_REPORT_DIR = Path.of("target", "bench");
    private static final String WORKER_PROP = "worker-prop.";

    private static final Set<String> KNOWN =
            new TreeSet<>(
                    List.of(
                            "scenario",
                            "target",
                            "jdbc-url",
                            "jdbc-user",
                            "jdbc-password",
                            "postgres-image",
                            "report-dir",
                            "name",
                            "events",
                            "event-types",
                            "lock-keys",
                            "workers",
                            "publisher-threads",
                            "handler-pool-size",
                            "claim-batch-size",
                            "poll-min-interval",
                            "poll-max-interval",
                            "executor",
                            "lock",
                            "finalize-batching",
                            "handler-work-time",
                            "failure-rate",
                            "workers-after-publish",
                            "drain-timeout",
                            "payload-bytes",
                            "connection-pool-size"));

    public BenchmarkOptions {
        Objects.requireNonNull(scenario, "scenario must not be null");
        Objects.requireNonNull(targetName, "targetName must not be null");
        Objects.requireNonNull(postgresImage, "postgresImage must not be null");
        Objects.requireNonNull(reportDir, "reportDir must not be null");
    }

    /** Parses {@code args}; see the class comment for the grammar. */
    public static BenchmarkOptions parse(String... args) {
        Map<String, String> kv = new LinkedHashMap<>();
        for (String arg : args) {
            if (!arg.startsWith(PREFIX)) {
                continue;
            }
            String body = arg.substring(PREFIX.length());
            int eq = body.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException(
                        "Expected " + PREFIX + "<key>=<value>, got '" + arg + "'");
            }
            String key = body.substring(0, eq).trim();
            if (!KNOWN.contains(key) && !key.startsWith(WORKER_PROP)) {
                throw new IllegalArgumentException(
                        "Unknown option '"
                                + key
                                + "'. Known keys: "
                                + KNOWN
                                + ", "
                                + WORKER_PROP
                                + "<property>");
            }
            kv.put(key, body.substring(eq + 1).trim());
        }

        Scenario base = Scenario.preset(kv.get("scenario"));
        Scenario.ScenarioBuilder b = base.toBuilder();
        apply(kv, "name", b::name);
        applyInt(kv, "events", b::events);
        applyInt(kv, "event-types", b::eventTypes);
        applyInt(kv, "lock-keys", b::lockKeyCardinality);
        applyInt(kv, "workers", b::workers);
        applyInt(kv, "publisher-threads", b::publisherThreads);
        applyInt(kv, "handler-pool-size", b::handlerPoolSize);
        applyInt(kv, "claim-batch-size", b::claimBatchSize);
        applyDuration(kv, "poll-min-interval", b::pollMinInterval);
        applyDuration(kv, "poll-max-interval", b::pollMaxInterval);
        apply(kv, "executor", v -> b.executorType(ExecutorType.parse(v)));
        apply(kv, "lock", v -> b.lockType(LockType.parse(v)));
        apply(kv, "finalize-batching", v -> b.finalizeBatching(bool("finalize-batching", v)));
        applyDuration(kv, "handler-work-time", b::handlerWorkTime);
        apply(kv, "failure-rate", v -> b.failureRate(Double.parseDouble(v)));
        apply(
                kv,
                "workers-after-publish",
                v -> b.workersStartAfterPublish(bool("workers-after-publish", v)));
        applyDuration(kv, "drain-timeout", b::drainTimeout);
        applyInt(kv, "payload-bytes", b::payloadBytes);
        applyInt(kv, "connection-pool-size", b::connectionPoolSize);

        Map<String, String> workerProps = new LinkedHashMap<>(base.workerProperties());
        kv.forEach(
                (k, v) -> {
                    if (k.startsWith(WORKER_PROP)) {
                        workerProps.put(k.substring(WORKER_PROP.length()), v);
                    }
                });
        b.workerProperties(workerProps);

        String jdbcUrl = kv.get("jdbc-url");
        DatabaseCoordinates database =
                jdbcUrl == null
                        ? null
                        : new DatabaseCoordinates(
                                jdbcUrl,
                                kv.getOrDefault("jdbc-user", "postgres"),
                                kv.getOrDefault("jdbc-password", ""));

        return new BenchmarkOptions(
                b.build(),
                kv.getOrDefault("target", DEFAULT_TARGET),
                database,
                kv.getOrDefault("postgres-image", DEFAULT_IMAGE),
                Path.of(kv.getOrDefault("report-dir", DEFAULT_REPORT_DIR.toString())));
    }

    private static void apply(
            Map<String, String> kv, String key, java.util.function.Consumer<String> setter) {
        String v = kv.get(key);
        if (v != null) {
            setter.accept(v);
        }
    }

    private static void applyInt(
            Map<String, String> kv, String key, java.util.function.IntConsumer setter) {
        apply(kv, key, v -> setter.accept(Integer.parseInt(v)));
    }

    private static void applyDuration(
            Map<String, String> kv, String key, java.util.function.Consumer<Duration> setter) {
        apply(kv, key, v -> setter.accept(DurationStyle.detectAndParse(v)));
    }

    private static boolean bool(String key, String v) {
        return switch (v.toLowerCase()) {
            case "true", "on", "yes" -> true;
            case "false", "off", "no" -> false;
            default -> throw new IllegalArgumentException(key + " must be true or false, got " + v);
        };
    }
}
