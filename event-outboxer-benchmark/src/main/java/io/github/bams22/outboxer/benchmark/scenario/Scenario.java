/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.benchmark.scenario;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import org.jspecify.annotations.Nullable;

/**
 * Everything that shapes one benchmark run. A scenario is a value: the report embeds it verbatim,
 * so a number is never separated from the configuration that produced it (ADR-0034 §7).
 *
 * <p><b>Construction.</b> Start from a preset ({@link #preset(String)}) and override with {@code
 * toBuilder()}. Required: {@code name}, {@code events}. Every other component has a default
 * substituted for {@code null} in the compact constructor, listed on the builder method. The
 * defaults are harness defaults, not production defaults: {@code pollMinInterval} is 100 ms where
 * the starter ships 500 ms, because steady-state latency is one of the things being measured.
 *
 * <p>Lock keys with {@code lockType=NOOP} are legal on purpose: that is the hot-key baseline, where
 * overlapping handlings are expected and reported rather than graded.
 *
 * @param name preset or user-chosen label; part of the report file name
 * @param events events to publish; sequence numbers run {@code 0..events-1}
 * @param eventTypes distinct event types ({@code BENCH_0..}); events are spread round-robin
 * @param lockKeyCardinality distinct lock keys; {@code 0} = handlers return no lock key
 * @param workers worker instances in the fleet
 * @param publisherThreads concurrent publishing threads, one transaction per event each
 * @param handlerPoolSize per-type handler pool ({@code handler-pool-size})
 * @param claimBatchSize events claimed per poll ({@code claim-batch-size})
 * @param pollMinInterval floor of the adaptive poll interval
 * @param pollMaxInterval ceiling of the adaptive poll interval
 * @param executorType platform or virtual handler threads
 * @param lockType entity locker in use
 * @param finalizeBatching group-commit finalize batching on or off (ADR-0014 amendment); boxed so
 *     that "unset" can take the library default, {@code true}
 * @param handlerWorkTime simulated work per handling ({@code Thread.sleep})
 * @param failureRate share of events whose <em>first</em> attempt returns {@code retry}, 0..1
 * @param workersStartAfterPublish {@code true} = backlog mode: publish everything, then start the
 *     fleet
 * @param drainTimeout how long to wait for every event to be handled before giving up
 * @param payloadBytes approximate serialized payload size (padding string length); {@code 0} takes
 *     the default of 256
 * @param connectionPoolSize Hikari pool size per context (publisher and each worker)
 * @param workerProperties extra starter properties applied last to every worker context
 * @param fleet in-process contexts or forked JVMs
 * @param workerJvmArgs JVM options for forked workers; ignored in-process
 * @param chaos what goes wrong on purpose during the drain
 */
@Builder(toBuilder = true)
public record Scenario(
        String name,
        int events,
        int eventTypes,
        int lockKeyCardinality,
        int workers,
        int publisherThreads,
        int handlerPoolSize,
        int claimBatchSize,
        Duration pollMinInterval,
        Duration pollMaxInterval,
        ExecutorType executorType,
        LockType lockType,
        Boolean finalizeBatching,
        Duration handlerWorkTime,
        double failureRate,
        boolean workersStartAfterPublish,
        Duration drainTimeout,
        int payloadBytes,
        int connectionPoolSize,
        Map<String, String> workerProperties,
        FleetMode fleet,
        List<String> workerJvmArgs,
        Chaos chaos) {

    /** Names of the shipped presets, in documentation order. */
    public static final List<String> PRESETS =
            List.of("smoke", "throughput", "hot-key", "failures", "backlog", "crash", "pg-restart");

    /**
     * Maintenance settings every chaos preset applies: the production defaults (30 s dead
     * threshold, 5 min stale-claim sweep, 10 min lock TTL) would turn a recovery into a coffee
     * break. Each value respects the starter's validation rules ({@code dead-threshold >= 3 ×
     * heartbeat}, {@code lock-ttl >= handler-max-runtime}). Documented in the report because they
     * bound the recovery times the run measures.
     */
    public static final Map<String, String> FAST_RECOVERY_PROPERTIES =
            Map.of(
                    "event-outboxer.maintenance.heartbeat-interval", "1s",
                    "event-outboxer.maintenance.dead-threshold", "5s",
                    "event-outboxer.maintenance.orphan-recovery-interval", "2s",
                    "event-outboxer.maintenance.watchdog-interval", "2s",
                    "event-outboxer.maintenance.stale-claim-sweep-interval", "5s",
                    "event-outboxer.event-types.defaults.handler-max-runtime", "10s",
                    "event-outboxer.event-types.defaults.lock-ttl", "15s");

    public Scenario {
        Objects.requireNonNull(name, "name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        positive("events", events);
        eventTypes = defaultIfZero(eventTypes, 1);
        positive("eventTypes", eventTypes);
        nonNegative("lockKeyCardinality", lockKeyCardinality);
        workers = defaultIfZero(workers, 2);
        positive("workers", workers);
        publisherThreads = defaultIfZero(publisherThreads, 4);
        positive("publisherThreads", publisherThreads);
        handlerPoolSize = defaultIfZero(handlerPoolSize, 3);
        positive("handlerPoolSize", handlerPoolSize);
        claimBatchSize = defaultIfZero(claimBatchSize, 10);
        positive("claimBatchSize", claimBatchSize);
        pollMinInterval = pollMinInterval == null ? Duration.ofMillis(100) : pollMinInterval;
        pollMaxInterval = pollMaxInterval == null ? Duration.ofSeconds(1) : pollMaxInterval;
        positive("pollMinInterval", pollMinInterval);
        if (pollMaxInterval.compareTo(pollMinInterval) < 0) {
            throw new IllegalArgumentException("pollMaxInterval must be >= pollMinInterval");
        }
        executorType = executorType == null ? ExecutorType.PLATFORM : executorType;
        lockType = lockType == null ? LockType.NOOP : lockType;
        finalizeBatching = finalizeBatching == null ? Boolean.TRUE : finalizeBatching;
        handlerWorkTime = handlerWorkTime == null ? Duration.ZERO : handlerWorkTime;
        if (handlerWorkTime.isNegative()) {
            throw new IllegalArgumentException("handlerWorkTime must not be negative");
        }
        if (failureRate < 0.0 || failureRate > 1.0) {
            throw new IllegalArgumentException(
                    "failureRate must be within [0, 1], got " + failureRate);
        }
        drainTimeout = drainTimeout == null ? Duration.ofMinutes(10) : drainTimeout;
        positive("drainTimeout", drainTimeout);
        payloadBytes = defaultIfZero(payloadBytes, 256);
        nonNegative("payloadBytes", payloadBytes);
        connectionPoolSize = defaultIfZero(connectionPoolSize, 8);
        positive("connectionPoolSize", connectionPoolSize);
        workerProperties =
                workerProperties == null
                        ? Map.of()
                        : Collections.unmodifiableMap(new LinkedHashMap<>(workerProperties));
        fleet = fleet == null ? FleetMode.IN_PROCESS : fleet;
        workerJvmArgs = workerJvmArgs == null ? List.of("-Xmx1g") : List.copyOf(workerJvmArgs);
        chaos = chaos == null ? Chaos.none() : chaos;
        if (chaos.killWorkers() > 0 && fleet != FleetMode.FORKED) {
            throw new IllegalArgumentException(
                    "chaos.killWorkers requires fleet=forked: an in-process context cannot be"
                            + " killed honestly");
        }
        if (chaos.killWorkers() > workers) {
            throw new IllegalArgumentException(
                    "chaos.killWorkers must not exceed workers ("
                            + workers
                            + "), got "
                            + chaos.killWorkers());
        }
    }

    /** Resolves a preset by name; {@code null} or blank means {@code smoke}. */
    public static Scenario preset(@Nullable String name) {
        String key = name == null || name.isBlank() ? "smoke" : name.trim().toLowerCase();
        return switch (key) {
            case "smoke" -> smoke();
            case "throughput" -> throughput();
            case "hot-key", "hotkey" -> hotKey();
            case "failures" -> failures();
            case "backlog" -> backlog();
            case "crash" -> crash();
            case "pg-restart", "pgrestart", "pg_restart" -> pgRestart();
            default ->
                    throw new IllegalArgumentException(
                            "Unknown scenario '" + name + "', expected one of " + PRESETS);
        };
    }

    /** Does the plumbing work: tiny, fast, also the {@code -P it} test. */
    public static Scenario smoke() {
        return Scenario.builder()
                .name("smoke")
                .events(200)
                .workers(2)
                .lockKeyCardinality(16)
                .lockType(LockType.POSTGRES_LEASE)
                .drainTimeout(Duration.ofSeconds(90))
                .build();
    }

    /** The engine's own ceiling: no lock key, no simulated work, several types. */
    public static Scenario throughput() {
        return Scenario.builder()
                .name("throughput")
                .events(20_000)
                .eventTypes(4)
                .workers(3)
                .handlerPoolSize(4)
                .claimBatchSize(50)
                .publisherThreads(8)
                .build();
    }

    /**
     * Everything lands on a handful of lock keys: the cost of entity locking. Run once with {@code
     * --bench.lock=noop} for the baseline where overlaps are expected, then with the lease locker.
     */
    public static Scenario hotKey() {
        return Scenario.builder()
                .name("hot-key")
                .events(5_000)
                .workers(3)
                .lockKeyCardinality(8)
                .lockType(LockType.POSTGRES_LEASE)
                .handlerWorkTime(Duration.ofMillis(5))
                .build();
    }

    /**
     * A share of first attempts return {@code retry}: the retry path and what it costs the
     * database. The retry delay is shortened from the 5 s production default so the run finishes.
     */
    public static Scenario failures() {
        return Scenario.builder()
                .name("failures")
                .events(5_000)
                .workers(3)
                .failureRate(0.10)
                .workerProperties(
                        Map.of(
                                "event-outboxer.event-types.defaults.failure.base-delay", "200ms",
                                "event-outboxer.event-types.defaults.failure.max-delay", "1s",
                                "event-outboxer.event-types.defaults.failure.jitter", "0.1"))
                .build();
    }

    /** Drain rate after an outage: publish everything first, then start the fleet. */
    public static Scenario backlog() {
        return Scenario.builder()
                .name("backlog")
                .events(20_000)
                .workers(3)
                .claimBatchSize(50)
                .workersStartAfterPublish(true)
                .build();
    }

    /**
     * Two of three forked workers are {@code SIGKILL}ed mid-drain and replaced: orphan reclaim,
     * lease takeover and the duplicate accounting that follows. Backlog mode so the fleet has work
     * to lose. Recovery timers are shortened ({@link #FAST_RECOVERY_PROPERTIES}).
     */
    public static Scenario crash() {
        return Scenario.builder()
                .name("crash")
                .events(5_000)
                .workers(3)
                .lockKeyCardinality(32)
                .lockType(LockType.POSTGRES_LEASE)
                .handlerWorkTime(Duration.ofMillis(2))
                .workersStartAfterPublish(true)
                .fleet(FleetMode.FORKED)
                .chaos(
                        Chaos.builder()
                                .killWorkers(2)
                                .killAtProgress(0.3)
                                .respawnKilled(true)
                                .build())
                .workerProperties(FAST_RECOVERY_PROPERTIES)
                .build();
    }

    /**
     * PostgreSQL is fast-restarted under a forked fleet mid-drain: connection-pool recovery,
     * finalize failures, stale-claim sweep. Recovery timers are shortened ({@link
     * #FAST_RECOVERY_PROPERTIES}); {@code --bench.pg-restart=crash} makes it a crash with WAL
     * replay instead.
     */
    public static Scenario pgRestart() {
        return Scenario.builder()
                .name("pg-restart")
                .events(5_000)
                .workers(3)
                .lockKeyCardinality(32)
                .lockType(LockType.POSTGRES_LEASE)
                .handlerWorkTime(Duration.ofMillis(2))
                .workersStartAfterPublish(true)
                .fleet(FleetMode.FORKED)
                .chaos(
                        Chaos.builder()
                                .postgresRestart(PostgresRestart.FAST)
                                .postgresRestartAtProgress(0.4)
                                .build())
                .workerProperties(FAST_RECOVERY_PROPERTIES)
                .build();
    }

    /** The lock key for a sequence number under this scenario, {@code null} when keys are off. */
    public @Nullable String lockKeyFor(long seq) {
        return lockKeyCardinality == 0 ? null : "key-" + Math.floorMod(seq, lockKeyCardinality);
    }

    /** The event-type index for a sequence number (round-robin). */
    public int typeIndexFor(long seq) {
        return (int) Math.floorMod(seq, eventTypes);
    }

    private static int defaultIfZero(int value, int fallback) {
        return value == 0 ? fallback : value;
    }

    private static void positive(String field, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be > 0, got " + value);
        }
    }

    private static void nonNegative(String field, int value) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " must be >= 0, got " + value);
        }
    }

    private static void positive(String field, Duration value) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be > 0, got " + value);
        }
    }
}
